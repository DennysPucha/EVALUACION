#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

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

void merge(Producto *arr, int left, int mid, int right, Producto *temp) {
    int i = left, j = mid + 1, k = 0;

    while (i <= mid && j <= right) {
        if (arr[i].precio <= arr[j].precio) {
            temp[k++] = arr[i++];
        } else {
            temp[k++] = arr[j++];
        }
    }
    while (i <= mid) temp[k++] = arr[i++];
    while (j <= right) temp[k++] = arr[j++];

    for (i = left, k = 0; i <= right; i++, k++) {
        arr[i] = temp[k];
    }
}

void merge_sort_final(Producto *arr, int n) {
    Producto *temp = (Producto *)malloc(n * sizeof(Producto));
    if (!temp) {
        fprintf(stderr, "Error de memoria en merge_sort_final\n");
        return;
    }

    int curr_size, left_start;

    for (curr_size = 1; curr_size < n; curr_size *= 2) {
        for (left_start = 0; left_start < n - 1; left_start += 2 * curr_size) {
            int mid = left_start + curr_size - 1;
            int right_end = (left_start + 2 * curr_size - 1 < n - 1) ? left_start + 2 * curr_size - 1 : n -1;
            if (mid < right_end)
                merge(arr, left_start, mid, right_end, temp);
        }
    }

    free(temp);
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

int main(int argc, char **argv) {
    int rank, size;
    Producto *productos = malloc(sizeof(Producto) * MAX_PRODUCTOS);
    if (!productos) {
        fprintf(stderr, "No se pudo asignar memoria.\n");
        return 1;
    }
    Producto *subproductos = NULL;
    int n = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        FILE *f = fopen("../productos.csv", "r");
        if (!f) {
            perror("No se pudo abrir productos.csv");
            MPI_Abort(MPI_COMM_WORLD, 1);
            return 1;
        }
        char linea[MAX_LINEA];

        fgets(linea, sizeof(linea), f); // saltar encabezado

        while (fgets(linea, sizeof(linea), f) && n < MAX_PRODUCTOS) {
            if (parse_line(linea, &productos[n])) {
                n++;
            } else {
                fprintf(stderr, "Error parseando línea: %s\n", linea);
            }
        }
        fclose(f);
    }

    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int chunk = n / size;
    int resto = n % size;
    int my_count = (rank < resto) ? chunk + 1 : chunk;
    int *counts = NULL;
    int *displs = NULL;

    if (rank == 0) {
        counts = (int *)malloc(size * sizeof(int));
        displs = (int *)malloc(size * sizeof(int));
        int offset = 0;
        for (int i = 0; i < size; i++) {
            counts[i] = (i < resto) ? chunk + 1 : chunk;
            displs[i] = offset;
            offset += counts[i];
        }
    }

    subproductos = (Producto *)malloc(my_count * sizeof(Producto));

    MPI_Datatype MPI_PRODUCTO;
    int lengths[3] = {1, MAX_NOMBRE, 1};
    MPI_Aint displacements[3];
    Producto dummy;

    MPI_Aint base_address;
    MPI_Get_address(&dummy, &base_address);
    MPI_Get_address(&dummy.id, &displacements[0]);
    MPI_Get_address(&dummy.nombre, &displacements[1]);
    MPI_Get_address(&dummy.precio, &displacements[2]);
    displacements[0] -= base_address;
    displacements[1] -= base_address;
    displacements[2] -= base_address;

    MPI_Datatype types[3] = {MPI_INT, MPI_CHAR, MPI_FLOAT};
    MPI_Type_create_struct(3, lengths, displacements, types, &MPI_PRODUCTO);
    MPI_Type_commit(&MPI_PRODUCTO);

    MPI_Scatterv(productos, counts, displs, MPI_PRODUCTO,
                 subproductos, my_count, MPI_PRODUCTO,
                 0, MPI_COMM_WORLD);

    double start = MPI_Wtime();

    quicksort(subproductos, 0, my_count - 1);

    MPI_Gatherv(subproductos, my_count, MPI_PRODUCTO,
                productos, counts, displs, MPI_PRODUCTO,
                0, MPI_COMM_WORLD);

    if (rank == 0) {
        merge_sort_final(productos, n);
        double end = MPI_Wtime();

        printf("Ordenamiento paralelo completado en %.6f segundos.\n", end - start);

        guardar_csv("productos_ordenados_mpi.csv", productos, n);
        printf("Archivo 'productos_ordenados_mpi.csv' generado.\n");

        free(counts);
        free(displs);
    }

    free(subproductos);
    MPI_Type_free(&MPI_PRODUCTO);
    MPI_Finalize();

    return 0;
}
