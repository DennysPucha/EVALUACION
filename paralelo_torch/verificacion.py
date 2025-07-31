import torch
print("¿PyTorch detecta CUDA?", torch.cuda.is_available())
print("Dispositivo actual:", torch.device('cuda' if torch.cuda.is_available() else 'cpu'))