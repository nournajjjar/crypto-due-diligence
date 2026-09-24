import requests

OLLAMA_API_URL = "http://host.docker.internal:11434/api/chat"
MODEL_NAME = "llama3"

def stream_crypto_response(user_input):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an expert in cryptocurrencies (Bitcoin, Ethereum, blockchain, DeFi, NFT, etc.). "
                    "If a question is not related to cryptocurrencies, respond with: "
                    "\"I don't know, I am only designed to answer questions about cryptocurrencies.\""
                )
            },
            {
                "role": "user",
                "content": user_input
            }
        ],
        "stream": True
    }

    with requests.post(OLLAMA_API_URL, json=payload, stream=True) as response:
        for line in response.iter_lines():
            if line:
                data = line.decode('utf-8').replace("data: ", "")
                yield data