import torch
import pandas as pd
import time
from torch.utils.data import Dataset, DataLoader

# Configuración de GPU
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Usando dispositivo: {device}")

# Clase para representar productos
class ProductoDataset(Dataset):
    def __init__(self, dataframe):
        self.data = dataframe
        
        # Convertir a tensores y mover a GPU
        self.ids = torch.tensor(dataframe['id'].values, dtype=torch.int32, device=device)
        self.nombres = dataframe['nombre'].values  # Los strings se manejan en CPU
        self.precios = torch.tensor(dataframe['precio'].values, dtype=torch.float32, device=device)
        
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        return {
            'id': self.ids[idx],
            'nombre': self.nombres[idx],
            'precio': self.precios[idx]
        }

def cargar_datos(archivo):
    df = pd.read_csv(archivo)
    return df

def ordenar_en_gpu(producto_dataset):
    # Obtener los precios como tensor en GPU
    precios = producto_dataset.precios
    ids = producto_dataset.ids
    
    # Obtener los índices ordenados
    indices_ordenados = torch.argsort(precios)
    
    # Aplicar el ordenamiento a todos los datos
    ids_ordenados = ids[indices_ordenados]
    precios_ordenados = precios[indices_ordenados]
    
    # Los nombres deben manejarse en CPU
    nombres_ordenados = [producto_dataset.nombres[i] for i in indices_ordenados.cpu().numpy()]
    
    return ids_ordenados.cpu().numpy(), nombres_ordenados, precios_ordenados.cpu().numpy()

def guardar_csv(nombre_archivo, ids, nombres, precios):
    df = pd.DataFrame({
        'id': ids,
        'nombre': nombres,
        'precio': precios
    })
    df.to_csv(nombre_archivo, index=False, encoding='utf-8')

def main():
    # Cargar datos
    print("Cargando datos...")
    df = cargar_datos('../productos.csv')
    
    # Crear dataset
    dataset = ProductoDataset(df)
    
    # Ordenar en GPU
    print("Ordenando en GPU...")
    inicio = time.time()
    
    ids_ordenados, nombres_ordenados, precios_ordenados = ordenar_en_gpu(dataset)
    
    fin = time.time()
    print(f"Ordenamiento completado en {fin - inicio:.6f} segundos.")
    
    # Guardar resultados
    print("Guardando resultados...")
    guardar_csv("productos_ordenados_gpu.csv", ids_ordenados, nombres_ordenados, precios_ordenados)
    print("Archivo 'productos_ordenados_gpu.csv' generado.")

if __name__ == "__main__":
    main()