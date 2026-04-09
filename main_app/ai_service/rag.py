import os
import uuid
import chromadb
import base64
import io
import db_config
from logger import logger
from shared import models
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer
from groq import Groq

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
LIMIT_COUNT = int(os.environ.get("LIMIT_COUNT"))


embedding_model = None

def get_model():
    global embedding_model
    if embedding_model is None:
        embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    return embedding_model

chroma_client = chromadb.PersistentClient(
    path="./chroma_db"
)



collection = chroma_client.get_or_create_collection(
    name="rag_tenant_documents"
)


def get_file_object_from_base64(data):
    file_bytes = base64.b64decode(data)
    file_obj = io.BytesIO(file_bytes)  # acts like a file
    return file_obj


def split_to_chunks(uploaded_file):
    reader = PdfReader(uploaded_file)
    extracted_text = ""
    chunks = []

    for page in reader.pages:
        extracted_text += page.extract_text()
    
    for i in range(0, len(extracted_text), 400):
        chunks.append(extracted_text[i:i+500])
    return chunks

    


def file_to_vector(tenant_id, uploaded_file, brand_ids):
    result = False
    message = ''
    try:
        file_path = os.path.join("/ai_service", uploaded_file)

        chunks = split_to_chunks(file_path)
        if not chunks:
            message = 'No chunks created.'
        else:
            
            embeddings = get_model().encode(chunks).tolist()
            if len(embeddings) != len(chunks):
                message = 'Embedding generation failed.'
            else:
                collection.add(documents=chunks,
                        embeddings=embeddings,
                        ids=[str(uuid.uuid4()) for _ in chunks],
                        metadatas=[
                            {
                                "tenant_id": int(tenant_id),
                                "brand_ids": [int(brand) for brand in brand_ids]
                            }
                            for _ in chunks
                        ])
                result = True
                message = 'Successfuly stored vectors'
    except Exception as e:
        result = False
        message = f'Error in generating: {e}'
    return result, message


def get_tone_instruction(tone: str):
    tone_map = {
        "Professional": "Respond in a clear, formal, and professional tone.",
        "Casual": "Respond in a relaxed and conversational tone.",
        "Friendly": "Respond in a warm, friendly, and helpful tone."
    }
    return tone_map.get(tone, "Respond in a helpful tone.")

def get_message_history(session_id):
    result = False
    history_list = list()
    try:
        db = next(db_config.get_db())
        chat_histories = (db.query(models.ChatHistory).filter(models.ChatHistory.session_id==session_id).order_by(models.ChatHistory.created_at.desc()).limit(LIMIT_COUNT).all())
        for history in chat_histories:
            history_info = {"role": "", "content": ""}
            if history.sender == "User":
                history_info["role"] = "user"
            else:
                history_info["role"] = "system"
            history_info["content"] = history.message

            history_list.append(history_info)
        result = True
    except Exception as e:
        result = False
        logger.exception(f'Error in generating: {e}')
    return result, history_list




def retrieve_relevant_chunks(tenant_id:int, tenant_brand_id:int, query, top_k=5):
    print('tenant_brand_id ', tenant_brand_id)
    print('query ', query)
    query_embedding = get_model().encode([query]).tolist()
    where_filter = {
        "$and": [
            {"tenant_id": tenant_id},
            {
                "brand_ids": {
                    "$contains": tenant_brand_id
                }
            }
        ]
    }
    results = collection.query(
        query_embeddings=query_embedding,
        n_results=top_k,
        where=where_filter
    )
    return results["documents"][0]


client = Groq(api_key=GROQ_API_KEY)

def generate_answer(session_id, query, brand_tone, context_chunks):
    print("context_chunks   ",context_chunks)
    context = "\n\n".join(context_chunks)

    tone_instruction = get_tone_instruction(brand_tone)

    prompt = f"""
    You are an AI assistant.

    {tone_instruction}

    Use the below context to answer the question.

    Context:
    {context}

    Question:
    {query}

    Answer:
    """

    messages = [
        {"role": "system", "content": prompt}
    ]

    history_result, history = get_message_history(session_id)
    if history_result and history:
        messages.extend(history)

    messages.append({"role": "user", "content": prompt})

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        temperature=0.2
    )

    return response.choices[0].message.content




