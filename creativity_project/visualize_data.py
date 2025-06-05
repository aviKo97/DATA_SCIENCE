import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import re
from collections import Counter, defaultdict
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


class RedditDataAnalyzer:
    def __init__(self, data_dir='data/raw', output_dir='visualizations'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.setup_output_dir()

        # Load all data
        self.all_posts = {}
        self.all_comments = {}
        self.load_all_data()

        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")

    def setup_output_dir(self):
        """Create output directory for visualizations"""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(f'{self.output_dir}/word_clouds', exist_ok=True)
        os.makedirs(f'{self.output_dir}/distributions', exist_ok=True)
        os.makedirs(f'{self.output_dir}/engagement', exist_ok=True)
        print(f"Output directory created: {self.output_dir}")

    def load_all_data(self):
        """Load all posts and comments data from JSON files"""
        print("Loading all Reddit data...")

        post_files = [f for f in os.listdir(self.data_dir) if f.endswith('_posts.json')]
        comment_files = [f for f in os.listdir(self.data_dir) if f.endswith('_comments.json')]

        # Load posts
        for file in post_files:
            subreddit = file.replace('_posts.json', '')
            file_path = os.path.join(self.data_dir, file)

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.all_posts[subreddit] = data.get('posts', [])
                    print(f"Loaded {len(self.all_posts[subreddit])} posts from r/{subreddit}")
            except Exception as e:
                print(f"Error loading {file}: {e}")

        # Load comments
        for file in comment_files:
            subreddit = file.replace('_comments.json', '')
            file_path = os.path.join(self.data_dir, file)

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    comments_dict = data.get('comments', {})

                    # Flatten comments into a list
                    comments_list = []
                    for post_id, post_comments in comments_dict.items():
                        for comment in post_comments:
                            comment['post_id'] = post_id
                            comments_list.append(comment)

                    self.all_comments[subreddit] = comments_list
                    print(f"Loaded {len(comments_list)} comments from r/{subreddit}")
            except Exception as e:
                print(f"Error loading {file}: {e}")

        print(f"\nTotal subreddits loaded: {len(self.all_posts)}")

    def run_sanity_checks(self):
        """Run comprehensive sanity checks on the data"""
        print("\n" + "=" * 50)
        print("RUNNING SANITY CHECKS")
        print("=" * 50)

        issues = []

        # Check and remove duplicate posts
        print("\n1. Checking and removing duplicate posts...")
        duplicates_removed = 0

        for subreddit, posts in self.all_posts.items():
            seen_ids = set()
            unique_posts = []

            for post in posts:
                post_id = post.get('id')
                if post_id and post_id not in seen_ids:
                    seen_ids.add(post_id)
                    unique_posts.append(post)
                elif post_id in seen_ids:
                    duplicates_removed += 1

            # Update with deduplicated posts
            original_count = len(self.all_posts[subreddit])
            self.all_posts[subreddit] = unique_posts
            removed_count = original_count - len(unique_posts)

            if removed_count > 0:
                print(f"  r/{subreddit}: Removed {removed_count} duplicate posts")

        if duplicates_removed > 0:
            print(f"✅ Removed {duplicates_removed} duplicate posts total")
            issues.append(f"Removed {duplicates_removed} duplicate posts")
        else:
            print("✅ No duplicate posts found")

        # Check and remove duplicate comments
        print("\n2. Checking and removing duplicate comments...")
        total_comment_duplicates = 0

        for subreddit, comments in self.all_comments.items():
            seen_ids = set()
            unique_comments = []

            for comment in comments:
                comment_id = comment.get('id')
                if comment_id and comment_id not in seen_ids:
                    seen_ids.add(comment_id)
                    unique_comments.append(comment)
                elif comment_id in seen_ids:
                    total_comment_duplicates += 1

            # Update with deduplicated comments
            original_count = len(self.all_comments[subreddit])
            self.all_comments[subreddit] = unique_comments
            removed_count = original_count - len(unique_comments)

            if removed_count > 0:
                print(f"  r/{subreddit}: Removed {removed_count} duplicate comments")

        if total_comment_duplicates > 0:
            print(f"✅ Removed {total_comment_duplicates} duplicate comments total")
            issues.append(f"Removed {total_comment_duplicates} duplicate comments")
        else:
            print("✅ No duplicate comments found")

        # Check for missing/empty content
        print("\n3. Checking for missing/empty content...")
        for subreddit, posts in self.all_posts.items():
            empty_titles = sum(1 for p in posts if not p.get('title', '').strip())
            if empty_titles > 0:
                issues.append(f"r/{subreddit}: {empty_titles} posts with empty titles")

        for subreddit, comments in self.all_comments.items():
            empty_bodies = sum(1 for c in comments if not c.get('body', '').strip())
            deleted_content = sum(1 for c in comments if c.get('body', '') in ['[deleted]', '[removed]'])
            if empty_bodies > 0:
                issues.append(f"r/{subreddit}: {empty_bodies} comments with empty bodies")
            if deleted_content > 0:
                print(f"⚠️  r/{subreddit}: {deleted_content} deleted/removed comments")

        # Check for score anomalies
        print("\n4. Checking for score anomalies...")
        for subreddit, posts in self.all_posts.items():
            zero_score_posts = sum(1 for p in posts if p.get('upvotes', 0) <= 0)
            if zero_score_posts > 0:
                issues.append(f"r/{subreddit}: {zero_score_posts} posts with ≤0 upvotes")

        for subreddit, comments in self.all_comments.items():
            negative_comments = sum(1 for c in comments if c.get('score', 0) < 0)
            if negative_comments > 0:
                print(f"📊 r/{subreddit}: {negative_comments} comments with negative scores")

        # Check author patterns
        print("\n5. Checking author patterns...")
        for subreddit, posts in self.all_posts.items():
            authors = [p.get('author', '') for p in posts if p.get('author') not in ['[deleted]', '']]
            if authors:
                author_counts = Counter(authors)
                top_author, top_count = author_counts.most_common(1)[0]
                if top_count > len(posts) * 0.1:  # If one author has >10% of posts
                    issues.append(
                        f"r/{subreddit}: User '{top_author}' dominates with {top_count} posts ({top_count / len(posts) * 100:.1f}%)")

        # Summary
        print(f"\n{'=' * 50}")
        print("SANITY CHECK SUMMARY")
        print(f"{'=' * 50}")

        if issues:
            print(f"⚠️  Found {len(issues)} potential issues:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("✅ All sanity checks passed!")

        # Save sanity check report
        with open(f'{self.output_dir}/sanity_check_report.txt', 'w') as f:
            f.write("Reddit Data Sanity Check Report\n")
            f.write("=" * 50 + "\n\n")
            if issues:
                f.write(f"Found {len(issues)} potential issues:\n\n")
                for issue in issues:
                    f.write(f"- {issue}\n")
            else:
                f.write("All sanity checks passed!\n")

        return issues

    def create_data_overview(self):
        """Create overview statistics table"""
        print("\n" + "=" * 50)
        print("DATA OVERVIEW")
        print("=" * 50)

        overview_data = []

        for subreddit in sorted(self.all_posts.keys()):
            posts = self.all_posts.get(subreddit, [])
            comments = self.all_comments.get(subreddit, [])

            post_upvotes = [p.get('upvotes', 0) for p in posts]
            comment_scores = [c.get('score', 0) for c in comments]

            overview_data.append({
                'Subreddit': f"r/{subreddit}",
                'Posts': len(posts),
                'Comments': len(comments),
                'Avg Post Upvotes': np.mean(post_upvotes) if post_upvotes else 0,
                'Avg Comment Score': np.mean(comment_scores) if comment_scores else 0,
                'Unique Authors (Posts)': len(
                    set(p.get('author', '') for p in posts if p.get('author') not in ['[deleted]', ''])),
                'Unique Authors (Comments)': len(
                    set(c.get('author', '') for c in comments if c.get('author') not in ['[deleted]', '']))
            })

        df = pd.DataFrame(overview_data)

        # Print formatted table
        print(df.to_string(index=False, float_format='%.1f'))

        # Save to CSV
        df.to_csv(f'{self.output_dir}/data_overview.csv', index=False)

        return df

    def plot_upvote_distributions(self):
        """Create upvote distribution plots for posts and comments"""
        print("\nCreating upvote distribution plots...")

        # Posts upvotes distribution
        fig, axes = plt.subplots(2, 1, figsize=(15, 12))

        # Prepare data for posts
        post_data = []
        for subreddit, posts in self.all_posts.items():
            for post in posts:
                post_data.append({
                    'subreddit': subreddit,
                    'upvotes': post.get('upvotes', 0)
                })

        post_df = pd.DataFrame(post_data)

        # Box plot for posts
        sns.boxplot(data=post_df, x='subreddit', y='upvotes', ax=axes[0])
        axes[0].set_title('Post Upvotes Distribution by Subreddit', fontsize=16, fontweight='bold')
        axes[0].set_xlabel('Subreddit')
        axes[0].set_ylabel('Upvotes')
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].set_yscale('log')  # Log scale for better visualization

        # Prepare data for comments
        comment_data = []
        for subreddit, comments in self.all_comments.items():
            for comment in comments:
                score = comment.get('score', 0)
                if score > 0:  # Only positive scores for log scale
                    comment_data.append({
                        'subreddit': subreddit,
                        'score': score
                    })

        comment_df = pd.DataFrame(comment_data)

        # Box plot for comments
        sns.boxplot(data=comment_df, x='subreddit', y='score', ax=axes[1])
        axes[1].set_title('Comment Scores Distribution by Subreddit', fontsize=16, fontweight='bold')
        axes[1].set_xlabel('Subreddit')
        axes[1].set_ylabel('Comment Score')
        axes[1].tick_params(axis='x', rotation=45)
        axes[1].set_yscale('log')

        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/distributions/upvote_distributions.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_word_clouds(self):
        """Create word clouds for each subreddit"""
        print("\nCreating word clouds...")

        # Common words to exclude
        stop_words = set(['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
                          'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                          'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must',
                          'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them',
                          'my', 'your', 'his', 'its', 'our', 'their', 'this', 'that', 'these', 'those',
                          'get', 'got', 'like', 'just', 'now', 'time', 'know', 'think', 'see', 'way',
                          'make', 'good', 'new', 'first', 'last', 'long', 'great', 'little', 'own', 'other',
                          'old', 'right', 'big', 'high', 'different', 'small', 'large', 'next', 'early',
                          'young', 'important', 'few', 'public', 'bad', 'same', 'able', 'reddit', 'edit',
                          'deleted', 'removed', 'http', 'https', 'www', 'com'])

        # Create word clouds for each subreddit
        for subreddit in self.all_posts.keys():
            print(f"Creating word cloud for r/{subreddit}...")

            # Combine all text from posts and comments
            all_text = []

            # Add post titles and content
            for post in self.all_posts[subreddit]:
                title = post.get('title', '') or ''
                content = post.get('content', '') or ''
                all_text.append(title)
                if content:
                    all_text.append(content)

            # Add comment bodies
            for comment in self.all_comments.get(subreddit, []):
                body = comment.get('body', '')
                if body and body not in ['[deleted]', '[removed]']:
                    all_text.append(body)

            # Clean and prepare text
            text = ' '.join(all_text)
            text = re.sub(r'http\S+', '', text)  # Remove URLs
            text = re.sub(r'[^a-zA-Z\s]', '', text)  # Keep only letters and spaces
            text = text.lower()

            # Remove stop words
            words = text.split()
            filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
            clean_text = ' '.join(filtered_words)

            if clean_text.strip():
                # Create word cloud
                wordcloud = WordCloud(
                    width=800,
                    height=400,
                    background_color='white',
                    max_words=100,
                    colormap='viridis'
                ).generate(clean_text)

                # Plot and save
                plt.figure(figsize=(12, 6))
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis('off')
                plt.title(f'Word Cloud for r/{subreddit}', fontsize=16, fontweight='bold')
                plt.tight_layout()
                plt.savefig(f'{self.output_dir}/word_clouds/{subreddit}_wordcloud.png', dpi=300, bbox_inches='tight')
                plt.show()

    def analyze_engagement_patterns(self):
        """Analyze and visualize engagement patterns"""
        print("\nAnalyzing engagement patterns...")

        engagement_data = []

        for subreddit in self.all_posts.keys():
            posts = self.all_posts[subreddit]
            comments = self.all_comments.get(subreddit, [])

            # Calculate metrics
            total_posts = len(posts)
            total_comments = len(comments)

            if total_posts > 0:
                avg_comments_per_post = total_comments / total_posts
                avg_post_upvotes = np.mean([p.get('upvotes', 0) for p in posts])

                # Calculate comment-to-upvote ratio
                total_post_upvotes = sum(p.get('upvotes', 0) for p in posts)
                comment_upvote_ratio = total_comments / total_post_upvotes if total_post_upvotes > 0 else 0

                # Average comment length
                comment_lengths = [len(c.get('body', '')) for c in comments if
                                   c.get('body') not in ['[deleted]', '[removed]']]
                avg_comment_length = np.mean(comment_lengths) if comment_lengths else 0

                engagement_data.append({
                    'subreddit': subreddit,
                    'avg_comments_per_post': avg_comments_per_post,
                    'avg_post_upvotes': avg_post_upvotes,
                    'comment_upvote_ratio': comment_upvote_ratio,
                    'avg_comment_length': avg_comment_length
                })

        engagement_df = pd.DataFrame(engagement_data)

        # Create engagement visualizations
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        # Comments per post
        sns.barplot(data=engagement_df, x='subreddit', y='avg_comments_per_post', ax=axes[0, 0])
        axes[0, 0].set_title('Average Comments per Post')
        axes[0, 0].tick_params(axis='x', rotation=90)

        # Average post upvotes
        sns.barplot(data=engagement_df, x='subreddit', y='avg_post_upvotes', ax=axes[0, 1])
        axes[0, 1].set_title('Average Post Upvotes')
        axes[0, 1].tick_params(axis='x', rotation=90)
        axes[0, 1].set_yscale('log')

        # Comment-to-upvote ratio
        sns.barplot(data=engagement_df, x='subreddit', y='comment_upvote_ratio', ax=axes[1, 0])
        axes[1, 0].set_title('Comments per Upvote Ratio')
        axes[1, 0].tick_params(axis='x', rotation=90)

        # Average comment length
        sns.barplot(data=engagement_df, x='subreddit', y='avg_comment_length', ax=axes[1, 1])
        axes[1, 1].set_title('Average Comment Length (characters)')
        axes[1, 1].tick_params(axis='x', rotation=90)

        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/engagement/engagement_patterns.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Save engagement data
        engagement_df.to_csv(f'{self.output_dir}/engagement_metrics.csv', index=False)

        return engagement_df

    def create_creativity_spectrum_preview(self):
        """Create a preview visualization suggesting creativity levels"""
        print("\nCreating creativity spectrum preview...")

        creativity_indicators = []

        for subreddit in self.all_posts.keys():
            posts = self.all_posts[subreddit]
            comments = self.all_comments.get(subreddit, [])

            # Simple heuristics for creativity (real analysis will use LLM)

            # 1. Vocabulary diversity (unique words / total words)
            all_text = []
            for post in posts:
                title = post.get('title', '') or ''
            content = post.get('content', '') or ''
            all_text.extend((title + ' ' + content).split())
            for comment in comments:
                if comment.get('body', '') not in ['[deleted]', '[removed]']:
                    all_text.extend(comment.get('body', '').split())

            vocab_diversity = len(set(all_text)) / len(all_text) if all_text else 0

            # 2. Average content length (longer = more detailed/creative?)
            content_lengths = []
            for post in posts:
                content = post.get('content', '')
                if content:
                    content_lengths.append(len(content))
            for comment in comments:
                body = comment.get('body', '')
                if body not in ['[deleted]', '[removed]']:
                    content_lengths.append(len(body))

            avg_content_length = np.mean(content_lengths) if content_lengths else 0

            # 3. Question vs statement ratio (questions might indicate more discussion)
            question_count = 0
            total_sentences = 0
            for post in posts:
                title = post.get('title', '') or ''
                content = post.get('content', '') or ''
                text = title + ' ' + content
                sentences = text.split('.')
                total_sentences += len(sentences)
                question_count += text.count('?')

            question_ratio = question_count / total_sentences if total_sentences > 0 else 0

            creativity_indicators.append({
                'subreddit': subreddit,
                'vocab_diversity': vocab_diversity,
                'avg_content_length': avg_content_length,
                'question_ratio': question_ratio,
                'estimated_creativity': vocab_diversity * 0.4 + (avg_content_length / 1000) * 0.3 + question_ratio * 0.3
            })

        creativity_df = pd.DataFrame(creativity_indicators)
        creativity_df = creativity_df.sort_values('estimated_creativity', ascending=False)

        # Plot creativity spectrum
        plt.figure(figsize=(14, 8))
        bars = plt.bar(range(len(creativity_df)), creativity_df['estimated_creativity'])

        # Color bars by creativity level
        colors = plt.cm.RdYlGn(creativity_df['estimated_creativity'] / creativity_df['estimated_creativity'].max())
        for bar, color in zip(bars, colors):
            bar.set_color(color)

        plt.xlabel('Subreddits (sorted by estimated creativity)')
        plt.ylabel('Estimated Creativity Score')
        plt.title(
            'Estimated Creativity Spectrum (Preview)\nBased on Vocabulary Diversity, Content Length, and Question Ratio')
        plt.xticks(range(len(creativity_df)), [f"r/{s}" for s in creativity_df['subreddit']], rotation=45)
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/creativity_spectrum_preview.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Print ranking
        print("\nEstimated Creativity Ranking (highest to lowest):")
        for i, row in creativity_df.iterrows():
            print(f"{row.name + 1:2d}. r/{row['subreddit']:20s} (score: {row['estimated_creativity']:.3f})")

        creativity_df.to_csv(f'{self.output_dir}/estimated_creativity_ranking.csv', index=False)

        return creativity_df

    def run_full_analysis(self):
        """Run all analysis and visualization functions"""
        print("Starting comprehensive Reddit data analysis...")
        print("This may take a few minutes...\n")

        # Run sanity checks
        issues = self.run_sanity_checks()

        # Create data overview
        overview_df = self.create_data_overview()

        # Create visualizations
        self.plot_upvote_distributions()
        self.create_word_clouds()
        engagement_df = self.analyze_engagement_patterns()
        creativity_df = self.create_creativity_spectrum_preview()

        print(f"\n{'=' * 50}")
        print("ANALYSIS COMPLETE!")
        print(f"{'=' * 50}")
        print(f"All visualizations saved to: {self.output_dir}/")
        print("Generated files:")
        print("- sanity_check_report.txt")
        print("- data_overview.csv")
        print("- distributions/upvote_distributions.png")
        print("- word_clouds/[subreddit]_wordcloud.png (for each subreddit)")
        print("- engagement/engagement_patterns.png")
        print("- engagement_metrics.csv")
        print("- creativity_spectrum_preview.png")
        print("- estimated_creativity_ranking.csv")

        return {
            'issues': issues,
            'overview': overview_df,
            'engagement': engagement_df,
            'creativity': creativity_df
        }


def main():
    """Run the analysis"""
    analyzer = RedditDataAnalyzer()
    results = analyzer.run_full_analysis()

    print(f"\nAnalysis complete! Check the '{analyzer.output_dir}' folder for all outputs.")


if __name__ == "__main__":
    main()