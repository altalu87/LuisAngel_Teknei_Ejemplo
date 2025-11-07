# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType
)
from typing import Dict, Optional
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkDataLoader:
    """
    Clase responsable ÚNICAMENTE de cargar archivos CSV en un esquema estrella.
    
    Responsabilidades:
    - Inicializar Spark con configuración optimizada
    - Definir esquemas explícitos para cada tabla
    - Cargar archivos CSV a DataFrames de Spark
    - Validar que los archivos existan antes de cargar
    
    NO realiza limpieza, transformación ni exportación.
    """
    
    # Nombres estándar de las tablas en el esquema estrella
    DIMENSION_TABLES = ['dim_tiempo', 'dim_tienda', 'dim_producto']
    FACT_TABLES = ['fact_ventas']
    ALL_TABLES = DIMENSION_TABLES + FACT_TABLES
    
    def __init__(
        self,        
        app_name: str = "StarSchemaLoader",
        driver_memory: str = "8g",
        executor_memory: str = "8g",
        shuffle_partitions: int = 200,
        jdbc_driver_path: str = r"C:\sqljdbc_13.2.1.0_enu\sqljdbc_13.2\enu\jars\mssql-jdbc-13.2.1.jre8.jar"
    ):
        """
        Inicializa el cargador de datos con Spark.
        
        Args:
            app_name: Nombre de la aplicación Spark
            driver_memory: Memoria del driver (default: 8g)
            executor_memory: Memoria del executor (default: 8g)
            shuffle_partitions: Número de particiones para shuffle (default: 200)
        """     
        self.app_name = app_name
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.shuffle_partitions = shuffle_partitions
        self.spark: Optional[SparkSession] = None
        self.dataframes: Dict[str, DataFrame] = {}
        self.jdbc_driver_path = jdbc_driver_path
        self._initialize_spark()
    
    def _initialize_spark(self) -> None:
        """
        Configura e inicializa la sesión de Spark con optimizaciones.
        """
        logger.info(f"Inicializando SparkSession: {self.app_name}")
        
        try:
            self.spark = (
                SparkSession.builder
                .appName(self.app_name)
                .master("local[*]")
                .config("spark.jars", self.jdbc_driver_path)
                .config("spark.driver.memory", self.driver_memory)
                .config("spark.executor.memory", self.executor_memory)
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.network.timeout", "800s")
                .config("spark.executor.heartbeatInterval", "120s")
                .config("spark.python.worker.reuse", "true")
                .getOrCreate()
            )
            
            # Configurar nivel de log (reducir ruido)
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"SparkSession inicializada correctamente")
            logger.info(f"Spark Version: {self.spark.version}")
            
        except Exception as e:
            logger.error(f"Error al inicializar Spark: {e}")
            raise
    
    @staticmethod
    def _get_schema(table_name: str) -> StructType:
        """
        Define esquemas explícitos para cada tabla del modelo estrella.
        
        NOTA IMPORTANTE sobre tipos de datos:
        - Fecha_ID: StringType porque puede contener valores mixtos (int/str)
        - Precio, métricas: DoubleType para soportar decimales
        - IDs numéricos: IntegerType (se validará limpieza posterior)
        
        Args:
            table_name: Nombre de la tabla (dim_tiempo, dim_tienda, etc.)
            
        Returns:
            StructType con el esquema definido
            
        Raises:
            ValueError: Si el nombre de tabla no es reconocido
        """
        schemas = {
            'dim_tiempo': StructType([
                StructField("Fecha_ID", StringType(), nullable=True),  # Mixto int/str
                StructField("Fecha", StringType(), nullable=True),
                StructField("Anio", IntegerType(), nullable=True),
                StructField("Mes", IntegerType(), nullable=True),
                StructField("Nombre_Mes", StringType(), nullable=True),  # Inconsistencias
                StructField("Numero_Semana", IntegerType(), nullable=True),
                StructField("Trimestre", StringType(), nullable=True),
                StructField("Estacionalidad_Factor", DoubleType(), nullable=True),
            ]),
            
            'dim_tienda': StructType([
                StructField("Tienda_ID", IntegerType(), nullable=True),
                StructField("Region", StringType(), nullable=True),  # Mayúsculas/tildes
                StructField("Formato_Tienda", StringType(), nullable=True),
                StructField("Atractivo_Factor", DoubleType(), nullable=True),
            ]),
            
            'dim_producto': StructType([
                StructField("Producto_ID", IntegerType(), nullable=True),
                StructField("Marca", StringType(), nullable=True),
                StructField("Segmento", StringType(), nullable=True),
                StructField("Sabor", StringType(), nullable=True),
                StructField("Tipo_Envase", StringType(), nullable=True),
                StructField("Tamano", StringType(), nullable=True),
                StructField("Precio_Base", DoubleType(), nullable=True),
                StructField("Demanda_Factor", DoubleType(), nullable=True),
            ]),
            
            'fact_ventas': StructType([
                StructField("Tienda_ID", IntegerType(), nullable=True),
                StructField("Producto_ID", IntegerType(), nullable=True),
                StructField("Fecha_ID", StringType(), nullable=True),  # Debe coincidir con dim_tiempo
                StructField("Distribucion_Numerica", DoubleType(), nullable=True),
                StructField("Distribucion_Ponderada", DoubleType(), nullable=True),  # Nulls
                StructField("Precio", DoubleType(), nullable=True),  # Nulls/Outliers
                StructField("Out_Of_Stock_Flag", IntegerType(), nullable=True),
                StructField("Ventas_Volumen", IntegerType(), nullable=True),  # Nulls/Outliers
                StructField("Ventas_Valor", DoubleType(), nullable=True),  # Nulls
            ])
        }
        
        if table_name not in schemas:
            raise ValueError(
                f"Esquema no definido para '{table_name}'. "
                f"Tablas válidas: {list(schemas.keys())}"
            )
        
        return schemas[table_name]
    
    def _validate_file_exists(self, filepath: str) -> bool:
        """
        Valida que el archivo existe antes de intentar cargarlo.
        
        Args:
            filepath: Ruta al archivo
            
        Returns:
            True si existe, False si no
        """
        path = Path(filepath)
        exists = path.exists() and path.is_file()
        
        if not exists:
            logger.warning(f"Archivo no encontrado: {filepath}")
        
        return exists
    
    def load_table(
        self, 
        table_name: str, 
        filepath: str,
        delimiter: str = ",",
        encoding: str = "utf-8"
    ) -> Optional[DataFrame]:
        """
        Carga una tabla individual desde CSV a Spark DataFrame.
        
        Args:
            table_name: Nombre de la tabla (debe estar en ALL_TABLES)
            filepath: Ruta al archivo CSV
            delimiter: Separador de columnas (default: ',')
            encoding: Codificación del archivo (default: 'utf-8')
            
        Returns:
            DataFrame de Spark o None si falla
        """
        if table_name not in self.ALL_TABLES:
            logger.error(
                f"Tabla '{table_name}' no válida. "
                f"Opciones: {self.ALL_TABLES}"
            )
            return None
        
        if not self._validate_file_exists(filepath):
            return None
        
        try:
            schema = self._get_schema(table_name)
            
            logger.info(f"Cargando {table_name} desde: {filepath}")
            
            df = (
                self.spark.read
                .option("header", "true")
                .option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("mode", "PERMISSIVE")  # Permite registros malformados
                .option("ignoreLeadingWhiteSpace", "true")
                .option("ignoreTrailingWhiteSpace", "true")
                .schema(schema)
                .csv(filepath)
            )
            
            # Almacenar en el diccionario interno
            self.dataframes[table_name] = df
            
            logger.info(f"Tabla {table_name} cargada correctamente")
            
            return df
            
        except Exception as e:
            logger.error(f"Error al cargar {table_name}: {e}")
            return None
    
    def load_all_tables(
        self, 
        base_path: str = ".",
        filename_prefix: str = "datos_bi_sucio_bebidas"
    ) -> Dict[str, DataFrame]:
        """
        Carga todas las tablas del esquema estrella automáticamente.
        
        Asume la convención de nombres:
        - {prefix}_dim_tiempo.csv
        - {prefix}_dim_tienda.csv
        - {prefix}_dim_producto.csv
        - {prefix}_fact_ventas.csv
        
        Args:
            base_path: Directorio donde están los archivos (default: '.')
            filename_prefix: Prefijo común de los archivos
            
        Returns:
            Diccionario con {table_name: DataFrame}
        """
        logger.info("Iniciando carga de todas las tablas del esquema estrella")
        logger.info(f"Ruta base: {base_path}")
        logger.info(f"Prefijo: {filename_prefix}")
        
        base = Path(base_path)
        loaded_count = 0
        
        for table_name in self.ALL_TABLES:
            filename = f"{filename_prefix}_{table_name}.csv"
            filepath = base / filename
            
            df = self.load_table(table_name, str(filepath))
            
            if df is not None:
                loaded_count += 1
        
        logger.info(
            f"Carga completada: {loaded_count}/{len(self.ALL_TABLES)} tablas"
        )
        
        if loaded_count < len(self.ALL_TABLES):
            missing = set(self.ALL_TABLES) - set(self.dataframes.keys())
            logger.warning(f"Tablas no cargadas: {missing}")
        
        return self.dataframes
    
    def get_dataframe(self, table_name: str) -> Optional[DataFrame]:
        """
        Obtiene un DataFrame cargado previamente.
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            DataFrame o None si no existe
        """
        if table_name not in self.dataframes:
            logger.warning(
                f"Tabla '{table_name}' no ha sido cargada. "
                f"Tablas disponibles: {list(self.dataframes.keys())}"
            )
            return None
        
        return self.dataframes[table_name]
    
    def show_loaded_tables(self) -> None:
        """
        Muestra un resumen de las tablas cargadas con conteo de registros.
        """
        if not self.dataframes:
            logger.info("No hay tablas cargadas")
            return
        
        logger.info("\n" + "="*60)
        logger.info("RESUMEN DE TABLAS CARGADAS")
        logger.info("="*60)
        
        for table_name in sorted(self.dataframes.keys()):
            df = self.dataframes[table_name]
            count = df.count()
            logger.info(f"  {table_name:20s} -> {count:,} registros")
        
        logger.info("="*60 + "\n")
    
    def stop(self) -> None:
        """
        Detiene la sesión de Spark y libera recursos.
        """
        if self.spark:
            logger.info("Deteniendo SparkSession")
            self.spark.stop()
            self.spark = None
            logger.info("SparkSession detenida correctamente")


# # ==============================================================================
# # EJEMPLO DE USO
# # ==============================================================================
    
#     # 1. Crear el loader
#     loader = SparkDataLoader(
#         app_name="EsquemaEstrellaLoader",
#         driver_memory="8g",
#         shuffle_partitions=200
#     )
    
#     # 2. Cargar todas las tablas automáticamente
#     dataframes = loader.load_all_tables(
#         base_path=".",
#         filename_prefix="datos_bi_sucio_bebidas"
#     )
    
#     # 3. Mostrar resumen
#     loader.show_loaded_tables()
    
#     # 4. Acceder a una tabla específica
#     dim_tiempo = loader.get_dataframe('dim_tiempo')
#     if dim_tiempo:
#         print("\nPrimeras 5 filas de dim_tiempo:")
#         dim_tiempo.show(5)
    
#     # 5. Detener Spark
#     loader.stop()