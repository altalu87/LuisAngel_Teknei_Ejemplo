# -*- coding: utf-8 -*-
"""
SparkDataExporter
=================
Clase para exportar DataFrames limpios a SQL Server y CSV
"""

from pyspark.sql import DataFrame
from typing import Optional, Dict
import logging
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkDataExporter:
    """
    Clase para exportar DataFrames de Spark a múltiples destinos.
    
    Responsabilidades:
    - Exportar a SQL Server con reintentos y optimizaciones
    - Exportar a CSV con control de particiones
    - Validar configuraciones de conexión
    - Reportar métricas de exportación
    """
    
    @staticmethod
    def measure_time(func):
        """Decorador para medir tiempo de ejecución."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"⏳ Iniciando: {func.__name__}")
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"⏱️ Tiempo total en {func.__name__}: {elapsed:.2f} segundos ({elapsed/60:.2f} minutos)")
            return result
        return wrapper
    
    def __init__(
        self,
        jdbc_url: Optional[str] = None,
        jdbc_user: Optional[str] = None,
        jdbc_password: Optional[str] = None,
        jdbc_driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ):
        """
        Inicializa el exportador con configuración de SQL Server.
        
        Args:
            jdbc_url: URL de conexión JDBC 
                      Ejemplo: "jdbc:sqlserver://localhost:1433;databaseName=MiDB;encrypt=true;trustServerCertificate=true"
            jdbc_user: Usuario de SQL Server (ej: "sa")
            jdbc_password: Contraseña
            jdbc_driver: NOMBRE DE LA CLASE del driver JDBC (NO la ruta al JAR)
                        Default: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                        
                        ⚠️ IMPORTANTE: Este es el NOMBRE DE LA CLASE Java, NO la ruta al archivo JAR.
                        La ruta al JAR se configura en SparkDataLoader.
                        
                        Ejemplos de nombres válidos:
                        - SQL Server: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                        - PostgreSQL: "org.postgresql.Driver"
                        - MySQL: "com.mysql.cj.jdbc.Driver"
        """
        self.jdbc_url = jdbc_url
        self.jdbc_user = jdbc_user
        self.jdbc_password = jdbc_password
        self.jdbc_driver = jdbc_driver
        
        self.export_stats: Dict = {}
        
        if jdbc_url:
            logger.info("✓ Configuración SQL Server inicializada")
            logger.info(f"  Driver class: {jdbc_driver}")
    
    def validate_sql_config(self) -> bool:
        """
        Valida que la configuración de SQL Server esté completa.
        
        Returns:
            True si la config es válida, False si no
        """
        if not all([self.jdbc_url, self.jdbc_user, self.jdbc_password]):
            logger.error("❌ Configuración SQL incompleta. Se requiere: url, user, password")
            return False
        return True
    
    @measure_time
    def export_to_sql(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        batchsize: int = 10000,
        max_partitions: int = 8,
        retries: int = 3,
        create_table: bool = True
    ) -> bool:
        """
        Exporta un DataFrame a SQL Server con optimizaciones y reintentos.
        
        Args:
            df: DataFrame de Spark a exportar
            table_name: Nombre de la tabla destino en SQL Server
            mode: Modo de escritura ('append', 'overwrite'). Default: 'append'
            batchsize: Filas por lote JDBC. Default: 10000
            max_partitions: Particiones para escritura paralela. Default: 8
            retries: Número de reintentos. Default: 3
            create_table: Si True, crea la tabla automáticamente. Default: True
            
        Returns:
            True si exportación exitosa, False si falla
        """
        # Validar configuración
        if not self.validate_sql_config():
            return False
        
        if df is None:
            logger.error("❌ DataFrame es None")
            return False
        
        try:
            # Contar registros
            total_rows = df.count()
            logger.info(f"📊 Total de registros a exportar: {total_rows:,}")
            
            if total_rows == 0:
                logger.warning("⚠️ DataFrame vacío, no hay nada que exportar")
                return False
            
            # Reparticionar para escritura paralela
            logger.info(f"🔧 Reparticionando en {max_partitions} particiones...")
            df_repartitioned = df.repartition(max_partitions)
            
            # Intentar exportación con reintentos
            success = False
            attempt = 0
            last_error = None
            
            while not success and attempt < retries:
                attempt += 1
                logger.info(f"🚀 Intento {attempt}/{retries}: Exportando a SQL Server → {table_name}")
                
                try:
                    # Opciones de escritura JDBC
                    writer = df_repartitioned.write \
                        .format("jdbc") \
                        .option("url", self.jdbc_url) \
                        .option("dbtable", table_name) \
                        .option("user", self.jdbc_user) \
                        .option("password", self.jdbc_password) \
                        .option("driver", self.jdbc_driver) \
                        .option("batchsize", batchsize) \
                        .option("isolationLevel", "READ_UNCOMMITTED") \
                        .option("numPartitions", max_partitions) \
                        .option("queryTimeout", 0)
                    
                    # Aplicar modo
                    if mode == "overwrite":
                        writer = writer.mode("overwrite")
                    else:
                        writer = writer.mode("append")
                    
                    # Ejecutar escritura
                    writer.save()
                    
                    success = True
                    logger.info(f"✅✅✅ EXPORTACIÓN EXITOSA A SQL SERVER ✅✅✅")
                    logger.info(f"📊 Total exportado: {total_rows:,} registros")
                    logger.info(f"📍 Tabla destino: {table_name}")
                    logger.info(f"⚙️ Modo: {mode}")
                    
                    # Guardar estadísticas
                    self.export_stats[table_name] = {
                        'rows': total_rows,
                        'mode': mode,
                        'attempts': attempt,
                        'success': True
                    }
                    
                except Exception as e:
                    last_error = e
                    logger.warning(f"⚠️ Error en intento {attempt}/{retries}: {str(e)}")
                    
                    if attempt < retries:
                        wait_time = 20 * attempt  # Backoff: 20s, 40s, 60s
                        logger.info(f"⏳ Esperando {wait_time}s antes de reintentar...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"❌ Fallo definitivo después de {retries} intentos")
                        logger.error(f"❌ Error: {str(last_error)}")
                        
                        self.export_stats[table_name] = {
                            'rows': total_rows,
                            'mode': mode,
                            'attempts': attempt,
                            'success': False,
                            'error': str(last_error)
                        }
                        
                        return False
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error crítico al exportar: {str(e)}")
            return False
    
    @measure_time
    def export_to_csv(
        self,
        df: DataFrame,
        output_path: str,
        header: bool = True,
        delimiter: str = ",",
        mode: str = "overwrite",
        num_files: int = 1,
        compression: Optional[str] = None
    ) -> bool:
        """
        Exporta un DataFrame a CSV.
        
        Args:
            df: DataFrame de Spark a exportar
            output_path: Ruta de salida (carpeta)
            header: Incluir encabezado. Default: True
            delimiter: Separador de columnas. Default: ','
            mode: Modo de escritura ('overwrite', 'append'). Default: 'overwrite'
            num_files: Número de archivos CSV resultantes. Default: 1
            compression: Compresión ('gzip', 'bzip2', None). Default: None
            
        Returns:
            True si exportación exitosa, False si falla
        """
        if df is None:
            logger.error("❌ DataFrame es None")
            return False
        
        try:
            total_rows = df.count()
            logger.info(f"📊 Total de registros a exportar: {total_rows:,}")
            
            if total_rows == 0:
                logger.warning("⚠️ DataFrame vacío, no hay nada que exportar")
                return False
            
            # Reparticionar según número de archivos deseado
            logger.info(f"🔧 Generando {num_files} archivo(s) CSV...")
            if num_files > 0:
                df = df.repartition(num_files)
            
            # Configurar escritura
            logger.info(f"🚀 Exportando a CSV → {output_path}")
            
            writer = df.write \
                .option("header", str(header).lower()) \
                .option("delimiter", delimiter) \
                .option("encoding", "UTF-8") \
                .mode(mode)
            
            # Aplicar compresión si se especifica
            if compression:
                writer = writer.option("compression", compression)
                logger.info(f"🗜️ Compresión habilitada: {compression}")
            
            # Ejecutar escritura
            writer.csv(output_path)
            
            logger.info(f"✅✅✅ EXPORTACIÓN EXITOSA A CSV ✅✅✅")
            logger.info(f"📊 Total exportado: {total_rows:,} registros")
            logger.info(f"📁 Ubicación: {output_path}")
            logger.info(f"📄 Archivos generados: {num_files}")
            
            # Guardar estadísticas
            self.export_stats[output_path] = {
                'rows': total_rows,
                'num_files': num_files,
                'compression': compression,
                'success': True
            }
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al exportar CSV: {str(e)}")
            
            self.export_stats[output_path] = {
                'rows': 0,
                'success': False,
                'error': str(e)
            }
            
            return False
    
    def export_multiple_tables_to_sql(
        self,
        tables: Dict[str, DataFrame],
        table_prefix: str = "",
        mode: str = "append",
        **kwargs
    ) -> Dict[str, bool]:
        """
        Exporta múltiples tablas a SQL Server.
        
        Args:
            tables: Diccionario {table_name: DataFrame}
            table_prefix: Prefijo para nombres de tabla. Default: ''
            mode: Modo de escritura. Default: 'append'
            **kwargs: Parámetros adicionales para export_to_sql()
            
        Returns:
            Diccionario {table_name: success_status}
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"EXPORTACIÓN MASIVA: {len(tables)} tablas a SQL Server")
        logger.info(f"{'='*70}")
        
        results = {}
        
        for table_name, df in tables.items():
            full_table_name = f"{table_prefix}{table_name}" if table_prefix else table_name
            
            logger.info(f"\n--- Exportando: {full_table_name} ---")
            success = self.export_to_sql(
                df=df,
                table_name=full_table_name,
                mode=mode,
                **kwargs
            )
            results[table_name] = success
        
        # Resumen
        successful = sum(1 for v in results.values() if v)
        logger.info(f"\n{'='*70}")
        logger.info(f"RESUMEN: {successful}/{len(tables)} tablas exportadas exitosamente")
        logger.info(f"{'='*70}")
        
        return results
    
    def export_multiple_tables_to_csv(
        self,
        tables: Dict[str, DataFrame],
        base_path: str,
        **kwargs
    ) -> Dict[str, bool]:
        """
        Exporta múltiples tablas a CSV.
        
        Args:
            tables: Diccionario {table_name: DataFrame}
            base_path: Ruta base donde crear carpetas por tabla
            **kwargs: Parámetros adicionales para export_to_csv()
            
        Returns:
            Diccionario {table_name: success_status}
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"EXPORTACIÓN MASIVA: {len(tables)} tablas a CSV")
        logger.info(f"{'='*70}")
        
        results = {}
        
        for table_name, df in tables.items():
            output_path = f"{base_path}/{table_name}"
            
            logger.info(f"\n--- Exportando: {table_name} ---")
            success = self.export_to_csv(
                df=df,
                output_path=output_path,
                **kwargs
            )
            results[table_name] = success
        
        # Resumen
        successful = sum(1 for v in results.values() if v)
        logger.info(f"\n{'='*70}")
        logger.info(f"RESUMEN: {successful}/{len(tables)} tablas exportadas exitosamente")
        logger.info(f"{'='*70}")
        
        return results
    
    def print_export_summary(self):
        """Imprime un resumen de todas las exportaciones realizadas."""
        if not self.export_stats:
            logger.info("No hay exportaciones registradas")
            return
        
        print("\n" + "="*70)
        print(" "*20 + "RESUMEN DE EXPORTACIONES")
        print("="*70)
        
        for name, stats in self.export_stats.items():
            status = "✓ Exitoso" if stats.get('success') else "✗ Fallido"
            print(f"\n{name}:")
            print(f"  Estado: {status}")
            print(f"  Registros: {stats.get('rows', 0):,}")
            
            if 'mode' in stats:  # SQL export
                print(f"  Modo: {stats.get('mode')}")
                print(f"  Intentos: {stats.get('attempts', 0)}")
            
            if 'num_files' in stats:  # CSV export
                print(f"  Archivos: {stats.get('num_files')}")
                print(f"  Compresión: {stats.get('compression', 'None')}")
            
            if not stats.get('success') and 'error' in stats:
                print(f"  Error: {stats.get('error')}")
        
        print("="*70 + "\n")