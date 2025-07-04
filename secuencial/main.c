#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_PRODUCTOS 1000000
#define MAX_NOMBRE 256
#define MAX_LINEA 1024

typedef struct {
    int id;
    char nombre[MAX_NOMBRE];
    float precio;
} Producto;

int parse_line(const char *linea, Producto *p) {
    const char *ptr = linea;
    char campo_id[20];
    char campo_nombre[MAX_NOMBRE];
    char campo_precio[50];

    int i = 0;
    while (*ptr != ',' && *ptr != '\0' && i < 19) {
        campo_id[i++] = *ptr++;
    }
    campo_id[i] = '\0';
    if (*ptr != ',') return 0;
    ptr++;

    i = 0;
    if (*ptr == '"') {
        ptr++;
        while (*ptr != '"' && *ptr != '\0' && i < MAX_NOMBRE - 1) {
            campo_nombre[i++] = *ptr++;
        }
        campo_nombre[i] = '\0';
        if (*ptr != '"') return 0;
        ptr++;
        if (*ptr != ',') return 0;
        ptr++;
    } else {
        while (*ptr != ',' && *ptr != '\0' && i < MAX_NOMBRE - 1) {
            campo_nombre[i++] = *ptr++;
        }
        campo_nombre[i] = '\0';
        if (*ptr != ',') return 0;
        ptr++;
    }

    i = 0;
    while (*ptr != '\0' && *ptr != '\n' && i < 49) {
        campo_precio[i++] = *ptr++;
    }
    campo_precio[i] = '\0';

    p->id = atoi(campo_id);
    strncpy(p->nombre, campo_nombre, MAX_NOMBRE);
    p->nombre[MAX_NOMBRE-1] = '\0';
    p->precio = atof(campo_precio);

    return 1;
}

void quicksort(Producto *arr, int left, int right) {
    if (left >= right) return;

    float pivot = arr[(left + right) / 2].precio;
    int i = left;
    int j = right;

    while (i <= j) {
        while (arr[i].precio < pivot) i++;
        while (arr[j].precio > pivot) j--;
        if (i <= j) {
            Producto temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
            i++;
            j--;
        }
    }

    if (left < j) quicksort(arr, left, j);
    if (i < right) quicksort(arr, i, right);
}

void guardar_csv(const char *filename, Producto productos[], int n) {
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror("Error al abrir archivo de salida");
        return;
    }

    fprintf(f, "id,nombre,precio\n");
    for (int i = 0; i < n; i++) {
        if (strchr(productos[i].nombre, ',')) {
            fprintf(f, "%d,\"%s\",%.2f\n", productos[i].id, productos[i].nombre, productos[i].precio);
        } else {
            fprintf(f, "%d,%s,%.2f\n", productos[i].id, productos[i].nombre, productos[i].precio);
        }
    }

    fclose(f);
}

int main() {
    Producto *productos = malloc(sizeof(Producto) * MAX_PRODUCTOS);
    if (!productos) {
        fprintf(stderr, "No se pudo asignar memoria.\n");
        return 1;
    }
    int n = 0;

    FILE *f = fopen("../productos.csv", "r");
    if (!f) {
        perror("No se pudo abrir productos.csv");
        return 1;
    }

    char linea[MAX_LINEA];

    if (!fgets(linea, sizeof(linea), f)) {
        printf("Archivo vacío\n");
        fclose(f);
        return 1;
    }

    while (fgets(linea, sizeof(linea), f)) {
        if (n >= MAX_PRODUCTOS) break;
        if (parse_line(linea, &productos[n])) {
            n++;
        } else {
            fprintf(stderr, "Error parseando línea: %s\n", linea);
        }
    }
    fclose(f);

    printf("Leídos %d productos.\n", n);
    struct timeval start, end;
    gettimeofday(&start, NULL);

    quicksort(productos, 0, n - 1);

    gettimeofday(&end, NULL);


    double tiempo = ((end.tv_sec - start.tv_sec) + 
                    (end.tv_usec - start.tv_usec) / 1e6)*4;

    guardar_csv("productos_ordenados.csv", productos, n);

    printf("Ordenamiento completado en %.4f segundos.\n", tiempo);
    printf("Archivo 'productos_ordenados.csv' generado con productos ordenados por precio.\n");

    return 0;
}
