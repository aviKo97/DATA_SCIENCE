import praw
import torch
import numpy as np
import nltk
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from transformers import GPT2LMHeadModel, GPT2TokenizerFast

nltk.download('punkt')

# ---------- הגדרות API ----------
REDDIT_CLIENT_ID = 'mzuTlgCKwtg1w3HFS3522Q'
REDDIT_SECRET = 'T7NBBHXz8jdhmNYiu-XHAlLbdXrpWw'
REDDIT_USER_AGENT = "script:creativity:v1.0 (by u/AggressiveJelly3323)"

# ---------- התחברות ל-Reddit ----------
reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID,
                     client_secret=REDDIT_SECRET,
                     user_agent=REDDIT_USER_AGENT)

# ---------- שליפת תגובות ----------
def fetch_comments(url, limit=50):
    submission = reddit.submission(url=url)
    submission.comments.replace_more(limit=0)
    comments = [comment.body.strip().replace("\n", " ") for comment in submission.comments[:limit]]
    return [c for c in comments if c]  # הסרת תגובות ריקות

# ---------- חישוב Perplexity ----------
def calculate_perplexity(texts):
    model_name = "gpt2"
    model = GPT2LMHeadModel.from_pretrained(model_name)
    tokenizer = GPT2TokenizerFast.from_pretrained(model_name)
    model.eval()

    ppl_scores = []
    for text in texts:
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            loss = model(**inputs, labels=inputs["input_ids"]).loss
        ppl = torch.exp(loss).item()
        ppl_scores.append(ppl)
    return ppl_scores

# ---------- חישוב יצירתיות ----------
def creativity_score(comments):
    lexical_richness = [len(set(c.split())) / max(len(c.split()), 1) for c in comments]

    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(comments)
    similarities = cosine_similarity(X)
    avg_similarity = [np.mean(similarities[i]) for i in range(len(comments))]
    uniqueness = [1 - s for s in avg_similarity]

    perplexities = calculate_perplexity(comments)
    normalized_ppl = [(min(perplexities) / p) for p in perplexities]

    scores = []
    for i in range(len(comments)):
        score = (
            0.4 * uniqueness[i] +
            0.3 * lexical_richness[i] +
            0.3 * normalized_ppl[i]
        )
        scores.append(score)

    df = pd.DataFrame({
        "comment": comments,
        "creativity_score": scores,
        "length": [len(c.split()) for c in comments],
        "lexical_richness": lexical_richness,
        "uniqueness": uniqueness,
        "perplexity": perplexities,
        "normalized_ppl": normalized_ppl
    })

    return df.sort_values(by="creativity_score", ascending=False)

# ---------- הפעלת הכל ----------
def analyze_post(url, out_file="reddit_creativity_scores.csv"):
    comments = fetch_comments(url)
    df = creativity_score(comments)
    df.to_csv(out_file, index=False)
    print(f"\n✅ נשמר {len(df)} תגובות בקובץ: {out_file}")
    print(df[["comment", "creativity_score"]].head(5))  # הדפסה של ה-5 הכי יצירתיות

# דוגמה לשימוש:
post_url = "https://www.reddit.com/r/Guitar/comments/skiz5a/discussion_whats_the_best_guitar_youve_ever_owned/"
analyze_post(post_url)
