import json
import pandas as pd
import os
from datetime import datetime
import textwrap

class RedditDataExplorer:
    def __init__(self, data_dir='data/raw'):
        """Initialize the data explorer"""
        self.data_dir = data_dir
        self.subreddits = ['art', 'AskEngineers', 'soccer', 'cooking', 'askreddit',
        'WritingPrompts', 'Showerthoughts', 'explainlikeimfive', 'relationships', 'LegalAdvice',
        'news', 'buildapc', 'politics', 'technology', 'personalfinance','relationship_advice',
                'computerscience', 'PhysicsStudents', 'premed', 'psychologystudents',
                           'philosophy', 'AcademicPhilosophy']
        self.posts_data = {}
        self.comments_data = {}
        self.load_all_data()

    def load_all_data(self):
        """Load all JSON data files"""
        print("Loading data...")

        for subreddit in self.subreddits:
            posts_file = f"{self.data_dir}/{subreddit}_posts.json"
            comments_file = f"{self.data_dir}/{subreddit}_comments.json"

            # Load posts
            if os.path.exists(posts_file):
                with open(posts_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.posts_data[subreddit] = data['posts']
                    print(f"✓ Loaded {len(data['posts'])} posts from r/{subreddit}")
            else:
                print(f"✗ Posts file not found for r/{subreddit}")
                self.posts_data[subreddit] = []

            # Load comments
            if os.path.exists(comments_file):
                with open(comments_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.comments_data[subreddit] = data['comments']
                    total_comments = sum(len(comments) for comments in data['comments'].values())
                    print(f"✓ Loaded {total_comments} comments from r/{subreddit}")
            else:
                print(f"✗ Comments file not found for r/{subreddit}")
                self.comments_data[subreddit] = {}

        print("\nData loading complete!\n")

    def show_summary(self):
        """Display overall data summary"""
        print("="*60)
        print("REDDIT DATA COLLECTION SUMMARY")
        print("="*60)

        total_posts = 0
        total_comments = 0

        for subreddit in self.subreddits:
            posts_count = len(self.posts_data.get(subreddit, []))
            comments_count = sum(len(comments) for comments in self.comments_data.get(subreddit, {}).values())

            total_posts += posts_count
            total_comments += comments_count

            print(f"r/{subreddit:<12} | {posts_count:>3} posts | {comments_count:>4} comments")

        print("-" * 60)
        print(f"{'TOTAL':<12} | {total_posts:>3} posts | {total_comments:>4} comments")
        print("="*60)

    def show_top_posts(self, subreddit=None, limit=10, min_upvotes=0):
        """Show top posts by upvotes"""
        if subreddit:
            subreddits = [subreddit] if subreddit in self.subreddits else []
        else:
            subreddits = self.subreddits

        all_posts = []
        for sub in subreddits:
            posts = self.posts_data.get(sub, [])
            for post in posts:
                if post['upvotes'] >= min_upvotes:
                    post['subreddit'] = sub
                    all_posts.append(post)

        # Sort by upvotes
        top_posts = sorted(all_posts, key=lambda x: x['upvotes'], reverse=True)[:limit]

        print(f"\nTOP {limit} POSTS" + (f" from r/{subreddit}" if subreddit else " (all subreddits)"))
        print("="*80)

        for i, post in enumerate(top_posts, 1):
            title = textwrap.fill(post['title'], width=60)
            print(f"{i:2}. [{post['upvotes']:>4}↑] r/{post['subreddit']} - {title}")
            if post['content'] and len(post['content']) > 0:
                content_preview = textwrap.fill(post['content'][:100] + "..." if len(post['content']) > 100 else post['content'], width=70, initial_indent="    ", subsequent_indent="    ")
                print(f"    {content_preview}")
            print()

    def show_post_details(self, subreddit, post_index):
        """Show detailed view of a specific post and its comments"""
        posts = self.posts_data.get(subreddit, [])

        if post_index < 1 or post_index > len(posts):
            print(f"Invalid post index. Please choose between 1 and {len(posts)}")
            return

        post = posts[post_index - 1]
        post_id = post['id']

        print("="*80)
        print(f"POST DETAILS - r/{subreddit}")
        print("="*80)
        print(f"Title: {post['title']}")
        print(f"Author: {post['author']} | Upvotes: {post['upvotes']} | Comments: {post['num_comments']}")
        print(f"Created: {datetime.fromtimestamp(post['created_utc']).strftime('%Y-%m-%d %H:%M')}")
        if post['flair']:
            print(f"Flair: {post['flair']}")

        if post['content']:
            print(f"\nContent:\n{textwrap.fill(post['content'], width=75)}")

        # Show comments
        comments = self.comments_data.get(subreddit, {}).get(post_id, [])
        if comments:
            print(f"\nTOP COMMENTS ({len(comments)}):")
            print("-" * 80)

            for i, comment in enumerate(comments[:10], 1):  # Show top 10 comments
                comment_text = textwrap.fill(comment['body'], width=70, initial_indent="  ", subsequent_indent="  ")
                print(f"{i:2}. [{comment['score']:>3}↑] {comment['author']}")
                print(f"{comment_text}\n")
        else:
            print("\nNo comments available for this post.")

    def search_posts(self, query, subreddit=None):
        """Search for posts containing specific text"""
        query = query.lower()
        results = []

        subreddits = [subreddit] if subreddit else self.subreddits

        for sub in subreddits:
            posts = self.posts_data.get(sub, [])
            for i, post in enumerate(posts):
                if query in post['title'].lower() or (post['content'] and query in post['content'].lower()):
                    results.append((sub, i + 1, post))

        print(f"\nSEARCH RESULTS for '{query}'" + (f" in r/{subreddit}" if subreddit else " (all subreddits)"))
        print("="*80)

        if not results:
            print("No results found.")
            return

        for sub, index, post in results[:20]:  # Show top 20 results
            title = textwrap.fill(post['title'], width=60)
            print(f"r/{sub} #{index} [{post['upvotes']:>4}↑] {title}")

    def show_subreddit_stats(self, subreddit):
        """Show detailed statistics for a specific subreddit"""
        posts = self.posts_data.get(subreddit, [])
        comments = self.comments_data.get(subreddit, {})

        if not posts:
            print(f"No data found for r/{subreddit}")
            return

        # Calculate stats
        upvotes = [post['upvotes'] for post in posts]
        comment_counts = [post['num_comments'] for post in posts]

        all_comments = []
        for post_comments in comments.values():
            all_comments.extend(post_comments)

        comment_scores = [c['score'] for c in all_comments] if all_comments else [0]

        print(f"\nSTATISTICS for r/{subreddit}")
        print("="*50)
        print(f"Posts collected: {len(posts)}")
        print(f"Comments collected: {len(all_comments)}")
        print(f"Average upvotes per post: {sum(upvotes)/len(upvotes):.1f}")
        print(f"Highest upvoted post: {max(upvotes)}")
        print(f"Average comments per post: {sum(comment_counts)/len(comment_counts):.1f}")
        if all_comments:
            print(f"Average comment score: {sum(comment_scores)/len(comment_scores):.1f}")
            print(f"Highest comment score: {max(comment_scores)}")

    def interactive_menu(self):
        """Main interactive menu"""
        while True:
            print("\n" + "="*60)
            print("REDDIT DATA EXPLORER")
            print("="*60)
            print("1. Show data summary")
            print("2. Show top posts (all subreddits)")
            print("3. Show top posts (specific subreddit)")
            print("4. View specific post details")
            print("5. Search posts")
            print("6. Show subreddit statistics")
            print("7. List available subreddits")
            print("0. Exit")
            print("-" * 60)

            choice = input("Enter your choice: ").strip()

            if choice == "0":
                print("Goodbye!")
                break
            elif choice == "1":
                self.show_summary()
            elif choice == "2":
                limit = input("How many posts to show? (default 10): ").strip()
                limit = int(limit) if limit.isdigit() else 10
                min_upvotes = input("Minimum upvotes? (default 0): ").strip()
                min_upvotes = int(min_upvotes) if min_upvotes.isdigit() else 0
                self.show_top_posts(limit=limit, min_upvotes=min_upvotes)
            elif choice == "3":
                subreddit = input("Enter subreddit name (art/AskEngineers/soccer/cooking/askreddit): ").strip()
                if subreddit in self.subreddits:
                    limit = input("How many posts to show? (default 10): ").strip()
                    limit = int(limit) if limit.isdigit() else 10
                    self.show_top_posts(subreddit=subreddit, limit=limit)
                else:
                    print("Invalid subreddit name!")
            elif choice == "4":
                subreddit = input("Enter subreddit name: ").strip()
                if subreddit in self.subreddits:
                    post_num = input(f"Enter post number (1-{len(self.posts_data.get(subreddit, []))}): ").strip()
                    if post_num.isdigit():
                        self.show_post_details(subreddit, int(post_num))
                    else:
                        print("Invalid post number!")
                else:
                    print("Invalid subreddit name!")
            elif choice == "5":
                query = input("Enter search term: ").strip()
                subreddit = input("Subreddit (leave empty for all): ").strip()
                subreddit = subreddit if subreddit in self.subreddits else None
                self.search_posts(query, subreddit)
            elif choice == "6":
                subreddit = input("Enter subreddit name: ").strip()
                if subreddit in self.subreddits:
                    self.show_subreddit_stats(subreddit)
                else:
                    print("Invalid subreddit name!")
            elif choice == "7":
                print("\nAvailable subreddits:")
                for sub in self.subreddits:
                    posts_count = len(self.posts_data.get(sub, []))
                    print(f"  - {sub} ({posts_count} posts)")
            else:
                print("Invalid choice! Please try again.")

def main():
    """Main function to run the data explorer"""
    print("Starting Reddit Data Explorer...")

    # Check if data directory exists
    if not os.path.exists('data/raw'):
        print("Error: data/raw directory not found!")
        print("Please run the data collection script first.")
        return

    explorer = RedditDataExplorer()
    explorer.interactive_menu()

if __name__ == "__main__":
    main()