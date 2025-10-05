# ==============================
# FAQ_Chatbot/scripts/marketing_faq_chatbot.py
# ==============================
#%pip install sentence-transformers
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import os

# ==============================
# 1️⃣ Load FAQ Data
# ==============================
def load_faq_data():
    """
    Load the marketing FAQ CSV file relative to the script location
    """
    data_path = os.path.join("../data/marketing_faq.csv")
    df = pd.read_csv(data_path, quotechar='"')
    return df

# ==============================
# 2️⃣ Load Hugging Face Model
# ==============================
def load_model():
    model = SentenceTransformer('all-MiniLM-L6-v2')
    return model

# ==============================
# 3️⃣ Compute Embeddings
# ==============================
def compute_embeddings(df, model):
    df['embedding'] = df['Answer'].apply(lambda x: model.encode(x))
    return df

# ==============================
# 4️⃣ Query Embedding
# ==============================
def get_query_embedding(query, model):
    return model.encode(query)

# ==============================
# 5️⃣ Find Most Similar FAQ Answer
# ==============================
def answer_query(query, df, model, top_k=1):
    query_emb = get_query_embedding(query, model)
    similarities = df['embedding'].apply(lambda x: cosine_similarity([query_emb], [x])[0][0])
    top_indices = similarities.sort_values(ascending=False).index[:top_k]
    return df.loc[top_indices, ['Question', 'Answer']]

# ==============================
# 6️⃣ Interactive Chat Loop
# ==============================
def chat_bot(df, model):
    print("Welcome to Marketing FAQ Chatbot! Type 'exit' to quit.")
    while True:
        query = input("You: ")
        if query.lower() in ["exit", "quit"]:
            print("Goodbye!")
            break
        response = answer_query(query, df, model)
        print("Bot:")
        print(response['Answer'].values[0])
        print("-" * 50)

# ==============================
# 7️⃣ Main Function
# ==============================
def main():
    df = load_faq_data()
    model = load_model()
    df = compute_embeddings(df, model)
    chat_bot(df, model)

# ==============================
# Entry Point
# ==============================
if __name__ == "__main__":
    main()
