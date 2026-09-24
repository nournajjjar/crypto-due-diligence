import requests
import uuid
import os

# Configuration des API (définies dans .env / variables d'environnement)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY")


def generate_script(prompt):
    """Génère un script avec l'API OpenAI"""
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "gpt-4",
        "messages": [
            {"role": "system", "content": "Crée un script court pour une vidéo basée sur le prompt."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 500
    }
    
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=data
    )
    
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    else:
        return f"Erreur de génération: {response.text}"

def get_pexels_video(query):
    """Récupère une vidéo depuis Pexels"""
    headers = {"Authorization": PEXELS_API_KEY}
    url = f"https://api.pexels.com/videos/search?query={query}&per_page=1"
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        videos = response.json().get("videos", [])
        if videos:
            video_files = videos[0].get("video_files", [])
            if video_files:
                # Retourne directement l'URL de la vidéo
                return max(video_files, key=lambda x: x.get("width", 0))["link"]
    return None

def generate_video_from_prompt(prompt):
    """Retourne simplement l'URL de la vidéo Pexels"""
    return get_pexels_video(prompt)