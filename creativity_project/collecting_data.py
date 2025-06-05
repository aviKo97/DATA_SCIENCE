import praw
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Set
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Subreddits to collect from
SUBREDDITS1 = ['art', 'AskEngineers', 'soccer', 'cooking', 'askreddit']
SUBREDDITS2 = ['WritingPrompts', 'Showerthoughts', 'explainlikeimfive', 'relationships', 'LegalAdvice']
SUBREDDITS3 = ['news', 'buildapc', 'politics', 'technology', 'personalfinance', 'relationship_advice',
               'computerscience', 'PhysicsStudents', 'premed', 'psychologystudents',
               'philosophy', 'AcademicPhilosophy']


class RedditDataCollector:
    def __init__(self, client_id: str, client_secret: str, user_agent: str, supplement_mode: bool = False):
        """Initialize Reddit API connection"""
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        # Subreddits to collect from
        self.subreddits = SUBREDDITS3 + SUBREDDITS2 + SUBREDDITS1
        self.supplement_mode = supplement_mode

        # Create data directories
        self.setup_directories()

        # Collection parameters
        self.posts_per_subreddit = 200
        self.comments_per_post = 20
        self.min_post_upvotes = 4 if supplement_mode else 5  # Lower threshold for supplement mode
        self.min_comment_score = 1 if supplement_mode else 2

        # Thresholds for supplement mode
        self.min_posts_threshold = 200
        self.min_comments_threshold = 500

    def setup_directories(self):
        """Create necessary directories for data storage"""
        directories = ['data/raw', 'data/processed', 'data/metadata']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        logger.info("Data directories created successfully")

    def load_existing_data(self, subreddit_name: str) -> tuple[List[Dict], Dict[str, List[Dict]], Set[str]]:
        """Load existing data for a subreddit and return existing post IDs"""
        posts_filename = f'data/raw/{subreddit_name}_posts.json'
        comments_filename = f'data/raw/{subreddit_name}_comments.json'

        existing_posts = []
        existing_comments = {}
        existing_post_ids = set()

        # Load existing posts
        if os.path.exists(posts_filename):
            try:
                with open(posts_filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_posts = data.get('posts', [])
                    existing_post_ids = {post['id'] for post in existing_posts}
                logger.info(f"Loaded {len(existing_posts)} existing posts from r/{subreddit_name}")
            except Exception as e:
                logger.error(f"Error loading existing posts for r/{subreddit_name}: {e}")

        # Load existing comments
        if os.path.exists(comments_filename):
            try:
                with open(comments_filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_comments = data.get('comments', {})
                total_existing_comments = sum(len(comments) for comments in existing_comments.values())
                logger.info(f"Loaded {total_existing_comments} existing comments from r/{subreddit_name}")
            except Exception as e:
                logger.error(f"Error loading existing comments for r/{subreddit_name}: {e}")

        return existing_posts, existing_comments, existing_post_ids

    def check_if_supplement_needed(self, subreddit_name: str) -> bool:
        """Check if a subreddit needs supplemental data collection"""
        if not self.supplement_mode:
            return True  # Always collect in normal mode

        existing_posts, existing_comments, _ = self.load_existing_data(subreddit_name)

        posts_count = len(existing_posts)
        comments_count = sum(len(comments) for comments in existing_comments.values())

        needs_posts = posts_count < self.min_posts_threshold
        needs_comments = comments_count < self.min_comments_threshold

        if needs_posts or needs_comments:
            logger.info(f"r/{subreddit_name} needs supplementing: {posts_count} posts, {comments_count} comments")
            return True
        else:
            logger.info(
                f"r/{subreddit_name} has sufficient data: {posts_count} posts, {comments_count} comments - skipping")
            return False

    def collect_post_data(self, post) -> Dict[str, Any]:
        """Extract relevant data from a Reddit post"""
        try:
            return {
                'id': post.id,
                'title': post.title,
                'content': post.selftext if post.selftext else None,
                'author': str(post.author) if post.author else '[deleted]',
                'created_utc': int(post.created_utc),
                'upvotes': post.score,
                'upvote_ratio': post.upvote_ratio,
                'num_comments': post.num_comments,
                'flair': post.link_flair_text,
                'url': post.url if not post.is_self else None,
                'is_self': post.is_self,
                'permalink': post.permalink
            }
        except Exception as e:
            logger.error(f"Error collecting post data for {post.id}: {e}")
            return None

    def collect_comment_data(self, comment) -> Dict[str, Any]:
        """Extract relevant data from a Reddit comment"""
        try:
            # Skip deleted/removed comments
            if comment.body in ['[deleted]', '[removed]'] or not comment.author:
                return None

            return {
                'id': comment.id,
                'body': comment.body,
                'author': str(comment.author),
                'created_utc': int(comment.created_utc),
                'score': comment.score,
                'parent_id': comment.parent_id if hasattr(comment, 'parent_id') else None,
                'depth': comment.depth if hasattr(comment, 'depth') else 0
            }
        except Exception as e:
            logger.error(f"Error collecting comment data: {e}")
            return None

    def collect_subreddit_posts(self, subreddit_name: str, existing_post_ids: Set[str] = None) -> List[Dict[str, Any]]:
        """Collect posts from a specific subreddit"""
        logger.info(f"Collecting posts from r/{subreddit_name}")

        if existing_post_ids is None:
            existing_post_ids = set()

        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            posts_data = []

            # Try different sorting methods for better coverage
            sort_methods = ['hot', 'top', 'best'] if self.supplement_mode else ['hot']

            for sort_method in sort_methods:
                if len(posts_data) >= self.posts_per_subreddit:
                    break

                logger.info(f"Trying {sort_method} posts from r/{subreddit_name}")

                # Get posts based on sorting method
                if sort_method == 'hot':
                    posts = subreddit.hot(limit=self.posts_per_subreddit * 3)
                elif sort_method == 'top':
                    posts = subreddit.new(limit=self.posts_per_subreddit * 3)
                elif sort_method == 'best':
                    posts = subreddit.top(time_filter='week', limit=self.posts_per_subreddit * 3)

                collected_count = len(posts_data)

                for post in posts:
                    if collected_count >= self.posts_per_subreddit:
                        break

                    # Skip if we already have this post
                    if post.id in existing_post_ids:
                        continue

                    # Filter by minimum upvotes
                    if post.score < self.min_post_upvotes:
                        continue

                    # Skip stickied posts (moderator announcements)
                    if post.stickied:
                        continue

                    post_data = self.collect_post_data(post)
                    if post_data:
                        posts_data.append(post_data)
                        existing_post_ids.add(post.id)  # Add to set to avoid duplicates
                        collected_count += 1

                        if collected_count % 20 == 0:
                            logger.info(f"Collected {collected_count} posts from r/{subreddit_name}")

                    # Rate limiting
                    time.sleep(0.1)

                logger.info(f"Got {len(posts_data)} posts from {sort_method} sorting")

            logger.info(f"Successfully collected {len(posts_data)} new posts from r/{subreddit_name}")
            return posts_data

        except Exception as e:
            logger.error(f"Error collecting posts from r/{subreddit_name}: {e}")
            return []

    def collect_post_comments(self, subreddit_name: str, post_id: str) -> List[Dict[str, Any]]:
        """Collect top comments from a specific post"""
        try:
            submission = self.reddit.submission(id=post_id)

            # Sort comments by score (best first)
            submission.comment_sort = 'top'
            submission.comments.replace_more(limit=0)  # Don't load "more comments"

            comments_data = []
            comment_count = 0

            for comment in submission.comments:
                if comment_count >= self.comments_per_post:
                    break

                # Filter by minimum score
                if comment.score < self.min_comment_score:
                    continue

                comment_data = self.collect_comment_data(comment)
                if comment_data:
                    comments_data.append(comment_data)
                    comment_count += 1

            return comments_data

        except Exception as e:
            logger.error(f"Error collecting comments for post {post_id}: {e}")
            return []

    def collect_subreddit_comments(self, subreddit_name: str, posts_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Collect comments for all posts in a subreddit"""
        logger.info(f"Collecting comments from r/{subreddit_name}")

        all_comments = {}

        for i, post in enumerate(posts_data):
            post_id = post['id']
            comments = self.collect_post_comments(subreddit_name, post_id)

            if comments:
                all_comments[post_id] = comments

            if (i + 1) % 10 == 0:
                logger.info(f"Collected comments for {i + 1}/{len(posts_data)} posts from r/{subreddit_name}")

            # Rate limiting - be gentle with Reddit's API
            time.sleep(0.2)

        total_comments = sum(len(comments) for comments in all_comments.values())
        logger.info(f"Successfully collected {total_comments} comments from r/{subreddit_name}")

        return all_comments

    def save_data(self, subreddit_name: str, posts_data: List[Dict], comments_data: Dict[str, List[Dict]],
                  existing_posts: List[Dict] = None, existing_comments: Dict[str, List[Dict]] = None):
        """Save collected data to JSON files, merging with existing data if in supplement mode"""
        timestamp = datetime.now().isoformat()

        # Merge with existing data if in supplement mode
        if self.supplement_mode and existing_posts is not None:
            all_posts = existing_posts + posts_data
            all_comments = existing_comments.copy()
            all_comments.update(comments_data)
        else:
            all_posts = posts_data
            all_comments = comments_data

        # Save posts
        posts_file = {
            'subreddit': subreddit_name,
            'collection_date': timestamp,
            'total_posts': len(all_posts),
            'posts': all_posts
        }

        posts_filename = f'data/raw/{subreddit_name}_posts.json'
        with open(posts_filename, 'w', encoding='utf-8') as f:
            json.dump(posts_file, f, indent=2, ensure_ascii=False)

        # Save comments
        comments_file = {
            'subreddit': subreddit_name,
            'collection_date': timestamp,
            'total_posts_with_comments': len(all_comments),
            'total_comments': sum(len(comments) for comments in all_comments.values()),
            'comments': all_comments
        }

        comments_filename = f'data/raw/{subreddit_name}_comments.json'
        with open(comments_filename, 'w', encoding='utf-8') as f:
            json.dump(comments_file, f, indent=2, ensure_ascii=False)

        logger.info(f"Data saved for r/{subreddit_name}: {posts_filename}, {comments_filename}")

        if self.supplement_mode:
            new_posts = len(posts_data)
            new_comments = sum(len(comments) for comments in comments_data.values())
            total_posts = len(all_posts)
            total_comments = sum(len(comments) for comments in all_comments.values())
            logger.info(
                f"Added {new_posts} new posts, {new_comments} new comments. Total: {total_posts} posts, {total_comments} comments")

    def collect_all_subreddits(self):
        """Main method to collect data from all subreddits"""
        mode_text = "supplement" if self.supplement_mode else "initial"
        logger.info(f"Starting Reddit data collection ({mode_text} mode)")

        collection_metadata = {
            'start_time': datetime.now().isoformat(),
            'mode': mode_text,
            'subreddits': self.subreddits,
            'posts_per_subreddit': self.posts_per_subreddit,
            'comments_per_post': self.comments_per_post,
            'min_post_upvotes': self.min_post_upvotes,
            'min_comment_score': self.min_comment_score,
            'results': {}
        }

        for subreddit_name in self.subreddits:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Processing r/{subreddit_name}")
            logger.info(f"{'=' * 50}")

            try:
                # Check if supplementing is needed
                if not self.check_if_supplement_needed(subreddit_name):
                    continue

                # Load existing data if in supplement mode
                existing_posts, existing_comments, existing_post_ids = [], {}, set()
                if self.supplement_mode:
                    existing_posts, existing_comments, existing_post_ids = self.load_existing_data(subreddit_name)

                # Collect posts
                posts_data = self.collect_subreddit_posts(subreddit_name, existing_post_ids)

                if not posts_data:
                    logger.warning(f"No new posts collected from r/{subreddit_name}")
                    if not self.supplement_mode:
                        continue

                # Collect comments
                comments_data = self.collect_subreddit_comments(subreddit_name, posts_data)

                # Save data
                self.save_data(subreddit_name, posts_data, comments_data, existing_posts, existing_comments)

                # Update metadata
                collection_metadata['results'][subreddit_name] = {
                    'posts_collected': len(posts_data),
                    'comments_collected': sum(len(comments) for comments in comments_data.values()),
                    'posts_with_comments': len(comments_data)
                }

                logger.info(f"Completed r/{subreddit_name}")

                # Longer pause between subreddits
                time.sleep(2)

            except Exception as e:
                logger.error(f"Failed to process r/{subreddit_name}: {e}")
                collection_metadata['results'][subreddit_name] = {'error': str(e)}

        # Save collection metadata
        collection_metadata['end_time'] = datetime.now().isoformat()
        metadata_filename = f'data/metadata/collection_info_{mode_text}.json'
        with open(metadata_filename, 'w', encoding='utf-8') as f:
            json.dump(collection_metadata, f, indent=2, ensure_ascii=False)

        logger.info("\n" + "=" * 50)
        logger.info(f"Data collection completed ({mode_text} mode)!")
        logger.info("=" * 50)

        # Print summary
        total_posts = sum(result.get('posts_collected', 0) for result in collection_metadata['results'].values())
        total_comments = sum(result.get('comments_collected', 0) for result in collection_metadata['results'].values())

        print(f"\nCollection Summary ({mode_text} mode):")
        print(f"New Posts: {total_posts}")
        print(f"New Comments: {total_comments}")
        for subreddit, result in collection_metadata['results'].items():
            if 'error' not in result:
                print(f"r/{subreddit}: {result['posts_collected']} posts, {result['comments_collected']} comments")


def main():
    """Main function to run the data collection"""
    # Reddit API credentials
    CLIENT_ID = "ovfc5VSCa2EsYgwtKIiSOw"
    CLIENT_SECRET = "RSsur3YtXdPr2tNXhD8GPpXeIv-q_g"
    USER_AGENT = "CreativityAnalysis/1.0 by Yuval"

    # Ask user for collection mode
    print("Reddit Data Collection")
    print("1. Initial collection (normal mode)")
    print("2. Supplement existing data (supplement mode)")

    choice = input("Choose mode (1 or 2): ").strip()

    supplement_mode = choice == "2"

    if supplement_mode:
        print("\nSupplement mode: Will only collect from subreddits with <200 posts or <500 comments")
        print("Lower thresholds will be used (1+ upvotes instead of 5+)")
    else:
        print("\nInitial mode: Will collect fresh data from all subreddits")

    # Initialize collector
    collector = RedditDataCollector(CLIENT_ID, CLIENT_SECRET, USER_AGENT, supplement_mode)

    # Start collection
    collector.collect_all_subreddits()


if __name__ == "__main__":
    main()