# Guía de Estudio para PySpark en Databricks

Documentación oficial Databricks [Abrir](https://api-docs.databricks.com/python/pyspark/latest/index.html)

Documentación oficial PySpark [Abrir](https://spark.apache.org/docs/latest/api/python/index.html)

## Índice
- [Plataforma Utilizada: Databricks Community](#plataforma-utilizada-databricks-community)
- [Requisitos](#requisitos)
- [Puntos del Examen y Guía Paso a Paso](#puntos-del-examen-y-guía-paso-a-paso)
  - [1. Trabajar con Notebooks en Databricks](#1-trabajar-con-notebooks-en-databricks)
  - [2. Limpieza de Datos (Data Cleaning)](#2-limpieza-de-datos-data-cleaning)
  - [3. Consultas con PySpark](#3-consultas-con-pyspark)
  - [4. Valor de Negocio de los Datos](#4-valor-de-negocio-de-los-datos)
  - [5. Ingeniería de Características](#5-ingeniería-de-características)
  - [6. Visualización de Datos](#6-visualización-de-datos)
- [Transformaciones Básicas en DataFrames](#transformaciones-básicas-en-dataframes)
- [Transformaciones Avanzadas en DataFrames](#transformaciones-avanzadas-en-dataframes)
- [Consejos Generales](#consejos-generales)

## Plataforma Utilizada: Databricks Community

Esta guía cubre los pasos necesarios para realizar un examen práctico sobre el uso de PySpark en la plataforma Databricks Community. Abarcaremos limpieza de datos, consultas, ingeniería de características, visualizaciones y una pregunta general sobre valor de negocio. Este material te será útil para familiarizarte con las funcionalidades básicas de PySpark en Databricks y prepararte para el examen.

## Requisitos
- Cuenta en Databricks Community: [Crear cuenta](https://community.cloud.databricks.com/login.html)
- Familiaridad con el uso de notebooks en Databricks.
- Conocimientos básicos de PySpark, Matplotlib y Seaborn.

## Puntos del Examen y Guía Paso a Paso

### 1. Trabajar con Notebooks en Databricks
- Utiliza notebooks de Databricks para crear código de PySpark, visualizar datos y realizar análisis.
- Familiarízate con el interfaz: cómo crear celdas de código, celdas Markdown para documentar, y cómo ejecutar cada celda.
- Aprende a usar **comandos magics** como `%fs` para interactuar con el sistema de archivos de Databricks y `%sql` para realizar consultas SQL directamente en el notebook.
- Utiliza **widgets** para hacer los notebooks interactivos. Por ejemplo:

  ```python
  dbutils.widgets.text("input", "default", "Ingrese un valor")
  valor = dbutils.widgets.get("input")
  print(valor)
  ```

### 2. Limpieza de Datos (Data Cleaning)
- **Formato de Datos**: CSV, Parquet, JSON, Avro, etc.
- **Carga de Datos**: Utiliza `spark.read.csv()`, `spark.read.parquet()`, `spark.read.json()`, etc. para cargar los datos. Ejemplo:

  ```python
  df = spark.read.csv('/FileStore/tables/archivo.csv', header=True, inferSchema=True)
  df_parquet = spark.read.parquet('/FileStore/tables/archivo.parquet')
  ```

- **Visualización Inicial**:
  - Utiliza `df.show()`, `df.printSchema()`, `df.describe().show()` para explorar los datos y entender su estructura.
- **Limpieza de Datos**:
  - Elimina valores nulos con `dropna()`. Puedes especificar columnas o usar el parámetro `how` para determinar si deben eliminarse filas con cualquier o todos los valores nulos:

    ```python
    df_clean = df.dropna(how='any')  # Elimina filas con al menos un valor nulo
    ```
  - Elimina duplicados con `dropDuplicates()`. Puedes especificar columnas específicas:

    ```python
    df_no_duplicates = df.dropDuplicates(['columna1', 'columna2'])
    ```
  - Filtra filas indeseadas con `filter()` o `where()`. Ejemplo:

    ```python
    df_filtered = df.filter(df['columna'] > valor)
    ```
  - **Manejo de valores atípicos** (outliers): Utiliza funciones como `approxQuantile()` para identificar y filtrar valores atípicos.

    ```python
    quantiles = df.approxQuantile('columna', [0.25, 0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    df_no_outliers = df.filter((df['columna'] >= (quantiles[0] - 1.5 * IQR)) & (df['columna'] <= (quantiles[1] + 1.5 * IQR)))
    ```

### 3. Consultas con PySpark
- **Consultas Básicas**:
  - Selecciona columnas: `df.select('columna').show()`
  - Filtra filas: `df.filter(df['columna'] == valor).show()`
  - Agrupa y calcula agregados: `df.groupBy('columna').count().show()`
  
- **Consultas Complejas**:
  - Utiliza funciones como `agg()`, `orderBy()`, `join()`, `distinct()` para realizar consultas complejas. Ejemplo:

    ```python
    df_aggregated = df.groupBy('columna').agg({'otra_columna': 'mean'}).orderBy('promedio', ascending=False)
    df_joined = df1.join(df2, df1['columna'] == df2['columna'], 'inner')
    ```
  - **Funciones de Ventana**: Utiliza `Window` para realizar cálculos como medias móviles o ranking.

    ```python
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    window_spec = Window.partitionBy('grupo').orderBy('valor')
    df = df.withColumn('rank', row_number().over(window_spec))
    ```
  - **Consultas SQL**: Puedes crear una vista temporal y realizar consultas SQL directamente:

    ```python
    df.createOrReplaceTempView("mi_vista")
    result = spark.sql("SELECT columna1, AVG(columna2) FROM mi_vista GROUP BY columna1")
    result.show()
    ```

### 4. Valor de Negocio de los Datos
- **Pregunta General**: ¿Qué valor de negocio se puede extraer?
  - Analiza qué información pueden aportar los datos en un contexto empresarial.
  - Identifica patrones y tendencias: esto puede ayudar a reducir costos, mejorar la experiencia del cliente, optimizar procesos, entre otros.
  - Ejemplo: Si los datos se refieren a ventas, podrías identificar los productos más vendidos, patrones de compra, o segmentos de clientes.
  - Considera también el **análisis predictivo**: ¿Es posible crear un modelo de predicción basado en los datos disponibles?

### 5. Ingeniería de Características
- **Creación de Nuevas Columnas**: A partir de las columnas existentes, crea nuevas métricas que puedan ser útiles para el análisis.
  - Utiliza `withColumn()` para agregar nuevas columnas.
  - Ejemplo: Si tienes una columna de precio y otra de cantidad, puedes crear una nueva columna de ingresos:

    ```python
    df = df.withColumn('ingresos', df['precio'] * df['cantidad'])
    ```
- **Funciones UDF** (User Defined Functions): Crea UDFs para aplicar transformaciones personalizadas a los datos.
  - Ejemplo:

    ```python
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    def convertir_euros(dolar):
        return dolar * 0.85

    convertir_euros_udf = udf(convertir_euros, DoubleType())
    df = df.withColumn('precio_euros', convertir_euros_udf(df['precio_dolar']))
    ```
- **One-Hot Encoding**: Utiliza `StringIndexer` y `OneHotEncoder` para transformar columnas categóricas en columnas numéricas.

  ```python
  from pyspark.ml.feature import StringIndexer, OneHotEncoder

  indexer = StringIndexer(inputCol="categoria", outputCol="categoria_index")
  df = indexer.fit(df).transform(df)

  encoder = OneHotEncoder(inputCol="categoria_index", outputCol="categoria_vec")
  df = encoder.fit(df).transform(df)
  ```
- **Normalización y Escalado**: Utiliza `MinMaxScaler` o `StandardScaler` para normalizar o escalar columnas numéricas.

  ```python
  from pyspark.ml.feature import MinMaxScaler
  from pyspark.ml.linalg import Vectors

  assembler = VectorAssembler(inputCols=["columna"], outputCol="features")
  df = assembler.transform(df)

  scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
  df = scaler.fit(df).transform(df)
  ```

### 6. Visualización de Datos
- Utiliza **Matplotlib** o **Seaborn** para crear visualizaciones.
- Puedes extraer datos del DataFrame de PySpark a Pandas para facilitar la visualización:

  ```python
  import matplotlib.pyplot as plt
  import seaborn as sns

  pandas_df = df.toPandas()
  sns.histplot(pandas_df['columna'])
  plt.show()
  ```
- **Gráficas Comunes**:
  - **Histogramas**: Para entender la distribución de una variable.
  - **Gráficas de líneas**: Para visualizar tendencias a lo largo del tiempo.
  - **Boxplots**: Para identificar outliers y entender la distribución de los datos.
  - **Heatmaps**: Para visualizar correlaciones entre variables.

- **Consejos**: La IA puede ayudarte a crear gráficas. Solicita ejemplos de código para generar gráficas específicas según tus necesidades.

## Transformaciones Básicas en DataFrames

### 1. `select()`
Selecciona una o varias columnas de un DataFrame.

**Ejemplo**:

```python
df_seleccionado = df.select("Nombre", "Edad")
df_seleccionado.show()
```

### 2. `filter()`
Filtra las filas del DataFrame que cumplen con una condición determinada.

**Ejemplo**:

```python
df_filtrado = df.filter(df["Salario"] > 40000)
df_filtrado.show()
```

### 3. `withColumn()`
Agrega una nueva columna o modifica una columna existente.

**Ejemplo**:

```python
from pyspark.sql.functions import col
df_modificado = df.withColumn("Salario Anual", col("Salario Mensual") * 12)
df_modificado.show()
```

### 4. `drop()`
Elimina una o más columnas del DataFrame.

**Ejemplo**:

```python
df_reducido = df.drop("Direccion")
df_reducido.show()
```

## Transformaciones Avanzadas en DataFrames

### 1. `groupBy()` y `agg()`
Agrupa filas según una columna y aplica funciones agregadas como `sum()`, `avg()`, etc.

**Ejemplo**:

```python
from pyspark.sql import functions
df_agrupado = df.groupBy("Departamento").agg(functions.avg("Salario").alias("Salario Medio"))
df_agrupado.show()
```

### 2. `join()`
Une dos DataFrames basándose en una clave común.

**Ejemplo**:

```python
df_unido = empleados_df.join(departamentos_df, empleados_df["dep_id"] == departamentos_df["id"], "left")
df_unido.show()
```

### 3. `orderBy()`
Ordena los datos por una o varias columnas.

**Ejemplo**:

```python
df_ordenado = df.orderBy("Edad", ascending=True)
df_ordenado.show()
```

### 4. `dropDuplicates()`
Elimina las filas duplicadas basándose en una o varias columnas.

**Ejemplo**:

```python
df_sin_duplicados = df.dropDuplicates(["Email"])
df_sin_duplicados.show()
```

### 5. `pivot()`
Reorganiza los datos convirtiendo los valores únicos de una columna en nuevas columnas.

**Ejemplo**:

```python
df_pivot = ventas_df.groupBy("Ciudad").pivot("Producto").sum("Cantidad")
df_pivot.show()
```

### 6. `explode()`
Expande una columna de tipo array en varias filas.

**Ejemplo**:

```python
from pyspark.sql.functions import explode
df_explotado = df.withColumn("elemento", explode(df["lista_elementos"]))
df_explotado.show()
```

### 7. `union()`
Une dos DataFrames con las mismas columnas.

**Ejemplo**:

```python
df_union = df1.union(df2)
df_union.show()
```

### 8. `crossJoin()`
Realiza un producto cartesiano entre dos DataFrames.

**Ejemplo**:

```python
df_cartesiano = df1.crossJoin(df2)
df_cartesiano.show()
```

### 9. `fillna()` / `replace()`
Rellena valores nulos o reemplaza valores en el DataFrame.

**Ejemplo**:

```python
df_rellenado = df.fillna({"Edad": 0, "Ciudad": "Desconocido"})
df_reemplazado = df.replace("N/A", "No Aplica", "Estado")
df_rellenado.show()
df_reemplazado.show()
```

### 10. `distinct()`
Devuelve un DataFrame sin filas duplicadas.

**Ejemplo**:

```python
df_distinto = df.distinct()
df_distinto.show()
```

### 11. `sample()`
Toma una muestra del DataFrame.

**Ejemplo**:

```python
df_muestra = df.sample(withReplacement=False, fraction=0.2)
df_muestra.show()
```

### 12. `rollup()` y `cube()`
Realizan agregaciones jerárquicas o multidimensionales.

**Ejemplo**:

```python
df_rollup = df.rollup("Categoria", "Subcategoria").sum("Ventas")
df_rollup.show()
df_cube = df.cube("Categoria", "Subcategoria").sum("Ventas")
df_cube.show()
```

### 13. Funciones de Ventana (`window()`)
Utiliza ventanas para realizar cálculos como sumas acumuladas o medias móviles.

**Ejemplo**:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum
window_spec = Window.partitionBy("Departamento").orderBy("Fecha")
df_ventana = df.withColumn("Ventas Acumuladas", sum("Ventas").over(window_spec))
df_ventana.show()
```

### 14. Transformaciones Condicionales (`when()` y `otherwise()`)
Permite realizar transformaciones basadas en condiciones.

**Ejemplo**:

```python
from pyspark.sql.functions import when
df_condicional = df.withColumn("Categoria_Riesgo", when(df["Edad"] < 18, "Bajo").otherwise("Alto"))
df_condicional.show()
```