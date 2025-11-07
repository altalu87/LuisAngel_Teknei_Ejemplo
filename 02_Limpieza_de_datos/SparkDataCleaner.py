# -*- coding: utf-8 -*-
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, Optional, Tuple
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkDataCleaner:
    """
    Clase responsable de la limpieza y transformación de datos del esquema estrella.
    
    Problemas que resuelve:
    1. Fecha_ID: Tipo mixto (int/str) → Estandarizar a string formato YYYYMMDD
    2. Nombre_Mes: Inconsistencias (Enero/ENERO/Ene) → Normalizar formato
    3. Region: Mayúsculas/minúsculas/tildes → Normalizar texto
    4. Precio: Valores negativos, cero, nulos → Eliminar o imputar
    5. Ventas_Volumen: Valores nulos, outliers → Eliminar o imputar
    6. Ventas_Valor: Valores nulos → Recalcular o eliminar
    7. Distribucion_Ponderada: Valores nulos → Imputar con mediana
    8. Integridad Referencial: IDs huérfanos → Eliminar o marcar
    """
    
    # Mapeo de nombres de meses normalizados
    MESES_NORMALIZADOS = {
        # Español completo
        'enero': 'Enero', 'febrero': 'Febrero', 'marzo': 'Marzo',
        'abril': 'Abril', 'mayo': 'Mayo', 'junio': 'Junio',
        'julio': 'Julio', 'agosto': 'Agosto', 'septiembre': 'Septiembre',
        'octubre': 'Octubre', 'noviembre': 'Noviembre', 'diciembre': 'Diciembre',
        # Abreviaciones
        'ene': 'Enero', 'feb': 'Febrero', 'mar': 'Marzo',
        'abr': 'Abril', 'may': 'Mayo', 'jun': 'Junio',
        'jul': 'Julio', 'ago': 'Agosto', 'sep': 'Septiembre',
        'oct': 'Octubre', 'nov': 'Noviembre', 'dic': 'Diciembre',
        # Abreviaciones variaciones
        'ene.': 'Enero', 'feb.': 'Febrero', 'mar.': 'Marzo',
        'abr.': 'Abril', 'may.': 'Mayo', 'jun.': 'Junio',
        'jul.': 'Julio', 'ago.': 'Agosto', 'sep.': 'Septiembre',
        'oct.': 'Octubre', 'nov.': 'Noviembre', 'dic.': 'Diciembre',        
        # Inglés (por si acaso)
        'january': 'Enero', 'february': 'Febrero', 'march': 'Marzo',
        'april': 'Abril', 'june': 'Junio', 'july': 'Julio',
        'august': 'Agosto', 'september': 'Septiembre', 'october': 'Octubre',
        'november': 'Noviembre', 'december': 'Diciembre',
    }
    
    # Mapeo de regiones normalizadas
    REGIONES_NORMALIZADAS = {
        'norte': 'Norte', 'sur': 'Sur', 'este': 'Este', 'oeste': 'Oeste',
        'centro': 'Centro', 'noreste': 'Noreste', 'noroeste': 'Noroeste',
        'sureste': 'Sureste', 'suroeste': 'Suroeste',
    }
    
    def __init__(self, dataframes: Dict[str, DataFrame]):
        """
        Inicializa el limpiador con los DataFrames cargados.
        
        Args:
            dataframes: Diccionario con {table_name: DataFrame}
        """
        self.original_dfs = dataframes
        self.cleaned_dfs: Dict[str, DataFrame] = {}
        self.cleaning_report: Dict[str, Dict] = {}
        
        logger.info("DataCleaner inicializado")
        logger.info(f"Tablas recibidas: {list(dataframes.keys())}")
    
    def _remove_accents(self, text: str) -> str:
        """
        Remueve tildes y acentos de un texto.
        
        Args:
            text: Texto a normalizar
            
        Returns:
            Texto sin acentos
        """
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
            'ñ': 'n', 'Ñ': 'N'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
    
    # =========================================================================
    # LIMPIEZA: DIM_TIEMPO
    # =========================================================================

    def clean_dim_tiempo(self, date_format: str = "yyyy-MM-dd") -> Optional[DataFrame]:
        """
        Limpia la dimensión tiempo.
        
        Problemas resueltos:
        1. Fecha: Convertir a DateType
        2. Fecha_ID: Estandarizar a string (remover espacios, validar formato)
        3. Nombre_Mes: Normalizar formato (Enero, Febrero, etc.)
        4. Validar coherencia entre Fecha, Anio, Mes
        
        Args:
            date_format: Formato de la columna Fecha (default: "yyyy-MM-dd")
                        Formatos comunes:
                        - "yyyy-MM-dd" → 2024-01-15
                        - "dd/MM/yyyy" → 15/01/2024
                        - "MM/dd/yyyy" → 01/15/2024
                        - "yyyyMMdd"   → 20240115
        
        Returns:
            DataFrame limpio o None si falla
        """
        logger.info("\n" + "="*60)
        logger.info("LIMPIANDO: dim_tiempo")
        logger.info("="*60)
        
        df = self.original_dfs.get('dim_tiempo')
        if df is None:
            logger.error("dim_tiempo no encontrado")
            return None
        
        initial_count = df.count()
        logger.info(f"Registros iniciales: {initial_count:,}")

        # 0. Convertir Fecha a DateType
        logger.info(f"Convirtiendo columna Fecha a DateType (formato: {date_format})")
        
        # Guardar Fecha original para comparación
        df = df.withColumn("Fecha_original", F.col("Fecha"))
        
        # Intentar conversión con formato especificado
        df = df.withColumn(
            "Fecha",
            F.to_date(F.col("Fecha"), date_format)
        )
        
        # Verificar conversiones fallidas
        fechas_invalidas = df.filter(F.col("Fecha").isNull() & F.col("Fecha_original").isNotNull()).count()
        
        if fechas_invalidas > 0:
            logger.warning(f"Fechas que no pudieron convertirse: {fechas_invalidas}")
            logger.warning("Mostrando ejemplos de fechas inválidas:")
            df.filter(F.col("Fecha").isNull() & F.col("Fecha_original").isNotNull()) \
              .select("Fecha_original", "Fecha_ID") \
              .show(10, truncate=False)
            
            # Intentar formatos alternativos automáticamente
            logger.info("Intentando formatos alternativos...")
            
            formatos_alternativos = [
                "dd/MM/yyyy",
                "MM/dd/yyyy", 
                "yyyyMMdd",
                "yyyy/MM/dd",
                "dd-MM-yyyy"
            ]
            
            for formato in formatos_alternativos:
                # Solo aplicar a las que fallaron
                df = df.withColumn(
                    "Fecha",
                    F.when(
                        F.col("Fecha").isNull() & F.col("Fecha_original").isNotNull(),
                        F.to_date(F.col("Fecha_original"), formato)
                    ).otherwise(F.col("Fecha"))
                )
                
                # Verificar cuántas se convirtieron
                pendientes = df.filter(F.col("Fecha").isNull() & F.col("Fecha_original").isNotNull()).count()
                convertidas = fechas_invalidas - pendientes
                
                if convertidas > 0:
                    logger.info(f"  Formato '{formato}': {convertidas} fechas convertidas")
                    fechas_invalidas = pendientes
                
                if pendientes == 0:
                    break
        
        # Estadísticas de conversión
        fechas_validas = df.filter(F.col("Fecha").isNotNull()).count()
        logger.info(f"Fechas convertidas exitosamente: {fechas_validas:,} ({fechas_validas/initial_count*100:.2f}%)")

        df = df.drop("Fecha_original")
        
        # 1. Limpiar Fecha_ID: remover espacios, convertir a string
        df = df.withColumn(
            "Fecha_ID",
            F.trim(F.col("Fecha_ID").cast("string"))
        )
        
        # 2. Validar y CORREGIR formato Fecha_ID (debe ser numérico de 8 dígitos: YYYYMMDD)
        df = df.withColumn(
            "Fecha_ID_valido",
            F.col("Fecha_ID").rlike("^[0-9]{8}$")
        )
        
        invalid_count = df.filter(~F.col("Fecha_ID_valido")).count()
        logger.info(f"Fecha_IDs inválidos encontrados: {invalid_count}")
        
        if invalid_count > 0:
            logger.warning("Mostrando ejemplos de Fecha_IDs inválidos:")
            df.filter(~F.col("Fecha_ID_valido")).select("Fecha_ID", "Fecha").show(10, truncate=False)
            
            # CORRECCIÓN: Reconstruir Fecha_ID desde la columna Fecha
            logger.info("Intentando corregir Fecha_IDs inválidos usando columna Fecha...")
            
            # Guardar Fecha_ID original para auditoría
            df = df.withColumn("Fecha_ID_original", F.col("Fecha_ID"))
            
            # Reconstruir Fecha_ID cuando:
            # 1. Fecha_ID sea inválido O sea nulo
            # 2. Y Fecha esté disponible
            df = df.withColumn(
                "Fecha_ID",
                F.when(
                    F.col("Fecha").isNotNull() & (
                        ~F.col("Fecha_ID_valido") | F.col("Fecha_ID").isNull()
                    ),
                    # Formato: YYYYMMDD (concatenar año, mes, día con padding)
                    F.concat(
                        F.lpad(F.year(F.col("Fecha")).cast("string"), 4, "0"),
                        F.lpad(F.month(F.col("Fecha")).cast("string"), 2, "0"),
                        F.lpad(F.dayofmonth(F.col("Fecha")).cast("string"), 2, "0")
                    )
                ).otherwise(F.col("Fecha_ID"))
            )
            
            # Contar cuántos se corrigieron
            corregidos = df.filter(
                F.col("Fecha_ID") != F.col("Fecha_ID_original")
            ).count()
            
            logger.info(f"Fecha_IDs corregidos desde columna Fecha: {corregidos}")
            
            # Verificar si aún quedan inválidos
            df = df.withColumn(
                "Fecha_ID_valido",
                F.col("Fecha_ID").rlike("^[0-9]{8}$")
            )
            
            aun_invalidos = df.filter(~F.col("Fecha_ID_valido")).count()
            
            if aun_invalidos > 0:
                logger.warning(f"Fecha_IDs que NO se pudieron corregir: {aun_invalidos}")
                logger.warning("Estos registros serán eliminados (Fecha_ID inválido y Fecha nula):")
                df.filter(~F.col("Fecha_ID_valido")).select(
                    "Fecha_ID_original", "Fecha_ID", "Fecha"
                ).show(10, truncate=False)
                
                # Eliminar solo los que no se pudieron corregir
                df = df.filter(F.col("Fecha_ID_valido"))
        
        df = df.drop("Fecha_ID_valido", "Fecha_ID_original")
        
        # 3. Normalizar Nombre_Mes
        # Crear mapeo en Spark
        meses_map = F.create_map([F.lit(x) for pair in self.MESES_NORMALIZADOS.items() for x in pair])
        
        df = df.withColumn(
            "Nombre_Mes_original",
            F.col("Nombre_Mes")
        )
        
        # Normalizar: minúsculas, trim, remover acentos
        df = df.withColumn(
            "Nombre_Mes_clean",
            F.lower(F.trim(F.col("Nombre_Mes")))
        )
        
        # Aplicar mapeo
        df = df.withColumn(
            "Nombre_Mes",
            F.coalesce(
                meses_map[F.col("Nombre_Mes_clean")],
                F.initcap(F.col("Nombre_Mes_clean"))  # Fallback: capitalizar
            )
        ).drop("Nombre_Mes_clean")

        df = df.drop("Nombre_Mes_original")
        
        # 6. Eliminar duplicados por Fecha_ID
        duplicates = df.groupBy("Fecha_ID").count().filter(F.col("count") > 1).count()
        if duplicates > 0:
            logger.warning(f"Duplicados por Fecha_ID: {duplicates}")
            df = df.dropDuplicates(["Fecha_ID"])
        
        final_count = df.count()
        removed = initial_count - final_count
        
        logger.info(f"Registros finales: {final_count:,}")
        logger.info(f"Registros eliminados: {removed:,} ({removed/initial_count*100:.2f}%)")
        
        # Guardar reporte
        self.cleaning_report['dim_tiempo'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed': removed,
            'invalid_fecha_id': invalid_count,
            #'incoherencias': incoherencias,
            'duplicates': duplicates
        }
        
        self.cleaned_dfs['dim_tiempo'] = df
        return df
    
    # =========================================================================
    # LIMPIEZA: DIM_TIENDA
    # =========================================================================

    def clean_dim_tienda(self) -> Optional[DataFrame]:
        """
        Limpia la dimensión tienda.
        
        Problemas resueltos:
        1. Region: Normalizar mayúsculas/minúsculas y tildes
        2. Validar Tienda_ID únicos
        3. Eliminar nulos en columnas críticas
        
        Returns:
            DataFrame limpio o None si falla
        """
        logger.info("\n" + "="*60)
        logger.info("LIMPIANDO: dim_tienda")
        logger.info("="*60)
        
        df = self.original_dfs.get('dim_tienda')
        if df is None:
            logger.error("dim_tienda no encontrado")
            return None
        
        initial_count = df.count()
        logger.info(f"Registros iniciales: {initial_count:,}")
        
        # 1. Normalizar Region
        # Crear mapeo en Spark
        regiones_map = F.create_map([F.lit(x) for pair in self.REGIONES_NORMALIZADAS.items() for x in pair])
        
        df = df.withColumn(
            "Region_original",
            F.col("Region")
        )
        
        # Normalizar: minúsculas, trim, remover acentos
        df = df.withColumn(
            "Region_clean",
            F.lower(F.trim(F.col("Region")))
        )
        
        # Remover tildes usando regexp_replace
        df = df.withColumn(
            "Region_clean",
            F.regexp_replace(F.col("Region_clean"), "[áàäâ]", "a")
        ).withColumn(
            "Region_clean",
            F.regexp_replace(F.col("Region_clean"), "[éèëê]", "e")
        ).withColumn(
            "Region_clean",
            F.regexp_replace(F.col("Region_clean"), "[íìïî]", "i")
        ).withColumn(
            "Region_clean",
            F.regexp_replace(F.col("Region_clean"), "[óòöô]", "o")
        ).withColumn(
            "Region_clean",
            F.regexp_replace(F.col("Region_clean"), "[úùüû]", "u")
        )
        
        # Aplicar mapeo
        df = df.withColumn(
            "Region",
            F.coalesce(
                regiones_map[F.col("Region_clean")],
                F.initcap(F.col("Region_clean"))  # Fallback
            )
        ).drop("Region_clean")
        
        # 2. Validar Tienda_ID únicos
        duplicates = df.groupBy("Tienda_ID").count().filter(F.col("count") > 1).count()
        if duplicates > 0:
            logger.warning(f"Tienda_IDs duplicados: {duplicates}")
            df = df.dropDuplicates(["Tienda_ID"])
                
        final_count = df.count()
        removed = initial_count - final_count
        
        logger.info(f"Registros finales: {final_count:,}")
        logger.info(f"Registros eliminados: {removed:,} ({removed/initial_count*100:.2f}%)")
        
        # Guardar reporte
        self.cleaning_report['dim_tienda'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed': removed,
            'duplicates': duplicates
        }
        
        self.cleaned_dfs['dim_tienda'] = df
        return df

    # =========================================================================
    # LIMPIEZA: DIM_PRODUCTO
    # =========================================================================
    
    def clean_dim_producto(self) -> Optional[DataFrame]:
        """
        Limpia la dimensión producto (considerada limpia, validaciones básicas).
        
        Validaciones:
        1. Producto_ID únicos
        2. Sin nulos en columnas críticas
        3. Precio_Base > 0
        
        Returns:
            DataFrame limpio o None si falla
        """
        logger.info("\n" + "="*60)
        logger.info("LIMPIANDO: dim_producto")
        logger.info("="*60)
        
        df = self.original_dfs.get('dim_producto')
        if df is None:
            logger.error("dim_producto no encontrado")
            return None
        
        initial_count = df.count()
        logger.info(f"Registros iniciales: {initial_count:,}")
        
        # 1. Validar Producto_ID únicos
        duplicates = df.groupBy("Producto_ID").count().filter(F.col("count") > 1).count()
        if duplicates > 0:
            logger.warning(f"Producto_IDs duplicados: {duplicates}")
            df = df.dropDuplicates(["Producto_ID"])
        
        # 2. Eliminar nulos en columnas críticas
        df = df.filter(
            F.col("Producto_ID").isNotNull() &
            F.col("Marca").isNotNull() &
            F.col("Precio_Base").isNotNull()
        )
        
        # 3. Validar Precio_Base > 0
        invalid_precio = df.filter(F.col("Precio_Base") <= 0).count()
        if invalid_precio > 0:
            logger.warning(f"Productos con Precio_Base <= 0: {invalid_precio}")
            df = df.filter(F.col("Precio_Base") > 0)
        
        final_count = df.count()
        removed = initial_count - final_count
        
        logger.info(f"Registros finales: {final_count:,}")
        logger.info(f"Registros eliminados: {removed:,} ({removed/initial_count*100:.2f}%)")
        
        # Guardar reporte
        self.cleaning_report['dim_producto'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed': removed,
            'duplicates': duplicates,
            'invalid_precio': invalid_precio
        }
        
        self.cleaned_dfs['dim_producto'] = df
        return df
    
    # =========================================================================
    # LIMPIEZA: FACT_VENTAS
    # =========================================================================
    
    def clean_fact_ventas(
        self,
        remove_null_precio: bool = False,        # CAMBIO: Más conservador
        remove_null_volumen: bool = False,       # CAMBIO: Más conservador
        impute_distribucion: bool = True,
        recalculate_valor: bool = True,
        remove_outliers: bool = False,
        outlier_std_threshold: float = 3.0
    ) -> Optional[DataFrame]:
        """
        Limpia la tabla de hechos.
        
        Problemas resueltos:
        1. Fecha_ID: Eliminar registros con formato inválido (no 8 dígitos numéricos)
        2. Precio: Eliminar negativos/cero/nulos
        3. Ventas_Volumen: Eliminar nulos, opcionalmente outliers
        4. Ventas_Valor: Recalcular como Precio * Volumen
        5. Distribucion_Ponderada: Imputar nulos con mediana
        
        Args:
            remove_null_precio: Eliminar registros con precio nulo
            remove_null_volumen: Eliminar registros con volumen nulo
            impute_distribucion: Imputar nulos en Distribucion_Ponderada
            recalculate_valor: Recalcular Ventas_Valor
            remove_outliers: Eliminar outliers en volumen
            outlier_std_threshold: Umbral de desviaciones estándar para outliers
            
        Returns:
            DataFrame limpio o None si falla
        """
        logger.info("\n" + "="*60)
        logger.info("LIMPIANDO: fact_ventas")
        logger.info("="*60)
        
        df = self.original_dfs.get('fact_ventas')
        if df is None:
            logger.error("fact_ventas no encontrado")
            return None
        
        initial_count = df.count()
        logger.info(f"Registros iniciales: {initial_count:,}")
        
        # =====================================================================
        # PASO 1: LIMPIAR FECHA_ID (ASIGNAR VALOR CENTINELA A INVÁLIDOS)
        # =====================================================================
        logger.info("\n--- PASO 1: Limpieza de Fecha_ID ---")
        
        # Valor centinela para fechas inválidas/desconocidas
        FECHA_CENTINELA = "19000101"  # 1 de enero de 1900 (fecha imposible en contexto de negocio)
        
        # Estandarizar: remover espacios y convertir a string
        df = df.withColumn(
            "Fecha_ID_original",
            F.col("Fecha_ID")
        )
        
        df = df.withColumn(
            "Fecha_ID",
            F.trim(F.col("Fecha_ID").cast("string"))
        )
        
        # Identificar Fecha_IDs válidos (8 dígitos numéricos)
        df = df.withColumn(
            "Fecha_ID_valido",
            F.col("Fecha_ID").rlike("^[0-9]{8}$")
        )
        
        # Contar inválidos
        fecha_id_invalidos = df.filter(~F.col("Fecha_ID_valido")).count()
        logger.info(f"Fecha_IDs inválidos encontrados: {fecha_id_invalidos:,}")
        
        if fecha_id_invalidos > 0:
            logger.warning(f"Mostrando ejemplos de Fecha_IDs inválidos (se asignará valor centinela: {FECHA_CENTINELA}):")
            df.filter(~F.col("Fecha_ID_valido")) \
              .select("Fecha_ID_original", "Fecha_ID", "Tienda_ID", "Producto_ID") \
              .show(10, truncate=False)
            
            # ASIGNAR VALOR CENTINELA en lugar de eliminar
            df = df.withColumn(
                "Fecha_ID",
                F.when(
                    ~F.col("Fecha_ID_valido") | F.col("Fecha_ID").isNull(),
                    F.lit(FECHA_CENTINELA)
                ).otherwise(F.col("Fecha_ID"))
            )
            
            logger.info(f"✓ Fecha_IDs corregidos con valor centinela: {fecha_id_invalidos:,}")
            logger.info(f"  Valor centinela usado: {FECHA_CENTINELA} (1900-01-01)")
        
        df = df.drop("Fecha_ID_valido")
        
        after_fecha_id = df.count()
        logger.info(f"Registros después de limpieza Fecha_ID: {after_fecha_id:,} (sin eliminaciones)")
        
        # =====================================================================
        # PASO 2: CALCULAR PRECIO PROMEDIO POR PRODUCTO (PARA IMPUTACIÓN)
        # =====================================================================
        logger.info("\n--- PASO 2: Calcular Precio Promedio por Producto ---")
        
        # Calcular precio promedio SOLO de transacciones válidas
        # (Ventas normales: Vol > 0, Precio > 0, OOS = 0)
        precio_promedio_producto = df.filter(
            (F.col("Ventas_Volumen") > 0) &
            (F.col("Precio") > 0) &
            (F.col("Out_Of_Stock_Flag") == 0)
        ).groupBy("Producto_ID").agg(
            F.avg("Precio").alias("Precio_Promedio_Producto")
        )
        
        # Join con fact_ventas
        df = df.join(precio_promedio_producto, "Producto_ID", "left")
        
        logger.info("✓ Precio promedio por producto calculado")
        
        # Verificar productos sin precio promedio
        productos_sin_precio = df.filter(F.col("Precio_Promedio_Producto").isNull()) \
            .select("Producto_ID").distinct().count()
        
        if productos_sin_precio > 0:
            logger.warning(f"Productos sin precio promedio válido: {productos_sin_precio}")
            logger.warning("Se usará la mediana global como fallback")
            
            # Calcular mediana global como fallback
            mediana_global = df.filter(F.col("Precio") > 0) \
                .approxQuantile("Precio", [0.5], 0.01)[0]
            
            df = df.withColumn(
                "Precio_Promedio_Producto",
                F.coalesce(F.col("Precio_Promedio_Producto"), F.lit(mediana_global))
            )
        
        # =====================================================================
        # PASO 3: CLASIFICAR TIPO DE TRANSACCIÓN Y DETECTAR ANOMALÍAS
        # =====================================================================
        logger.info("\n--- PASO 3: Clasificar Transacciones ---")
        
        """
        LÓGICA DE CLASIFICACIÓN (MEJORADA):
        
        1. VENTA_NORMAL:
           - Ventas_Volumen > 0
           - Precio > 0
           - Ventas_Valor > 0
           - Out_Of_Stock_Flag = 0
           
        2. DEVOLUCION:
           - Ventas_Volumen < 0
           - Precio > 0 (indica que hubo transacción monetaria)
           - Cualquier OOS (puede haber timing issues)
           
        3. AJUSTE_INVENTARIO:
           - Ventas_Volumen < 0
           - Out_Of_Stock_Flag = 1
           - Precio <= 0 o NULL (NO hubo transacción monetaria)
           - Representa: Merma, pérdida, ajuste administrativo
           
        4. VENTA_PERDIDA:
           - Ventas_Volumen = 0
           - Out_Of_Stock_Flag = 1
           - Ventas_Valor = 0 o NULL
           - Representa: Demanda no satisfecha por falta de stock
           
        5. REGISTRO_SIN_VENTA:
           - Ventas_Volumen = 0
           - Out_Of_Stock_Flag = 0
           - Ventas_Valor = 0 o NULL
           - Representa: Día sin ventas pero con stock disponible
           
        6. ANOMALO:
           - Combinaciones ilógicas no cubiertas arriba
        """
        
        df = df.withColumn(
            "Tipo_Transaccion",
            # 1. DEVOLUCION: Volumen negativo CON Precio válido (indica transacción monetaria)
            F.when(
                (F.col("Ventas_Volumen") < 0) & (F.col("Precio") > 0),
                F.lit("DEVOLUCION")
            )
            # 2. AJUSTE_INVENTARIO: Volumen negativo SIN Precio válido
            .when(
                (F.col("Ventas_Volumen") < 0) & 
                ((F.col("Precio") <= 0) | F.col("Precio").isNull()),
                F.lit("AJUSTE_INVENTARIO")
            )
            # 3. VENTA_PERDIDA: Vol=0, OOS=1, Valor válido
            .when(
                (F.col("Ventas_Volumen") == 0) &
                (F.col("Out_Of_Stock_Flag") == 1) &
                ((F.col("Ventas_Valor") == 0) | F.col("Ventas_Valor").isNull()),
                F.lit("VENTA_PERDIDA")
            )
            # 4. REGISTRO_SIN_VENTA: Vol=0, OOS=0, Valor=0
            .when(
                (F.col("Ventas_Volumen") == 0) &
                (F.col("Out_Of_Stock_Flag") == 0) &
                ((F.col("Ventas_Valor") == 0) | F.col("Ventas_Valor").isNull()),
                F.lit("REGISTRO_SIN_VENTA")
            )
            # 5. VENTA_NORMAL: Vol>0, Precio>0, coherente
            .when(
                (F.col("Ventas_Volumen") > 0) &
                (F.col("Precio") > 0) &
                (F.col("Ventas_Valor") > 0),
                F.lit("VENTA_NORMAL")
            )
            # 6. ANOMALO: Todo lo demás
            .otherwise(F.lit("ANOMALO"))
        )
        
        # Crear columna booleana para cada tipo
        df = df.withColumn("Es_Devolucion", F.col("Tipo_Transaccion") == "DEVOLUCION")
        df = df.withColumn("Es_Ajuste", F.col("Tipo_Transaccion") == "AJUSTE_INVENTARIO")
        df = df.withColumn("Es_Venta_Normal", F.col("Tipo_Transaccion") == "VENTA_NORMAL")
        df = df.withColumn("Es_Anomalo", F.col("Tipo_Transaccion") == "ANOMALO")
        
        # Reportar distribución
        tipo_distribucion = df.groupBy("Tipo_Transaccion").count().orderBy(F.col("count").desc())
        logger.info("Distribución de tipos de transacción:")
        tipo_distribucion.show(truncate=False)
        
        # =====================================================================
        # PASO 4: LIMPIAR Y CORREGIR PRECIO
        # =====================================================================
        logger.info("\n--- PASO 4: Limpieza y Corrección de Precio ---")
        
        # Guardar Precio original
        df = df.withColumn("Precio_Original", F.col("Precio"))
        
        # REGLA 1: Precio NULL → Imputar con precio promedio
        precio_null = df.filter(F.col("Precio").isNull()).count()
        logger.info(f"Registros con Precio NULL: {precio_null:,}")
        
        if precio_null > 0:
            df = df.withColumn(
                "Precio",
                F.coalesce(F.col("Precio"), F.col("Precio_Promedio_Producto"))
            )
            logger.info("✓ Precio NULL imputado con precio promedio del producto")
        
        # REGLA 2: Precio <= 0 → Imputar con precio promedio (solo si Vol > 0)
        precio_invalido = df.filter(
            (F.col("Precio_Original") <= 0) & (F.col("Ventas_Volumen") > 0)
        ).count()
        logger.info(f"Registros con Precio <= 0 y Volumen > 0: {precio_invalido:,}")
        
        if precio_invalido > 0:
            df = df.withColumn(
                "Precio",
                F.when(
                    (F.col("Precio") <= 0) & (F.col("Ventas_Volumen") > 0),
                    F.col("Precio_Promedio_Producto")
                ).otherwise(F.col("Precio"))
            )
            logger.info("✓ Precio <= 0 corregido con precio promedio (cuando Vol > 0)")
        
        # REGLA 3: Opcionalmente ELIMINAR registros con precio inválido
        if remove_null_precio:
            registros_antes = df.count()
            df = df.filter(
                F.col("Precio").isNotNull() & (F.col("Precio") > 0)
            )
            registros_despues = df.count()
            eliminados = registros_antes - registros_despues
            if eliminados > 0:
                logger.warning(f"✗ Registros eliminados por Precio inválido: {eliminados:,}")
        
        precio_negativo = precio_invalido
        precio_null_final = df.filter(F.col("Precio").isNull()).count()
        
        # =====================================================================
        # PASO 5: LIMPIAR Y CORREGIR VENTAS_VOLUMEN
        # =====================================================================
        logger.info("\n--- PASO 5: Limpieza de Ventas_Volumen ---")
        
        # REGLA: Volumen NULL → Eliminar (no se puede imputar volumen)
        volumen_null = df.filter(F.col("Ventas_Volumen").isNull()).count()
        logger.info(f"Registros con Ventas_Volumen NULL: {volumen_null:,}")
        
        if remove_null_volumen and volumen_null > 0:
            df = df.filter(F.col("Ventas_Volumen").isNotNull())
            logger.info(f"✓ Registros con Volumen NULL eliminados: {volumen_null:,}")
        
        # =====================================================================
        # PASO 6: RECALCULAR VENTAS_VALOR
        # =====================================================================
        logger.info("\n--- PASO 6: Recalcular Ventas_Valor ---")
        
        if recalculate_valor:
            # Guardar Ventas_Valor original
            df = df.withColumn("Ventas_Valor_Original", F.col("Ventas_Valor"))
            
            # RECALCULAR: Ventas_Valor = Precio * Ventas_Volumen
            # EXCEPTO para:
            # - VENTA_PERDIDA (mantener en 0)
            # - REGISTRO_SIN_VENTA (mantener en 0)
            df = df.withColumn(
                "Ventas_Valor",
                F.when(
                    F.col("Tipo_Transaccion").isin(["VENTA_PERDIDA", "REGISTRO_SIN_VENTA"]),
                    F.lit(0.0)
                ).otherwise(
                    F.col("Precio") * F.col("Ventas_Volumen")
                )
            )
            
            logger.info("✓ Ventas_Valor recalculado como Precio * Ventas_Volumen")
            
            # Detectar discrepancias significativas (>1% diferencia)
            if "Ventas_Valor_Original" in df.columns:
                discrepancias = df.filter(
                    F.col("Ventas_Valor_Original").isNotNull() &
                    (F.abs(F.col("Ventas_Valor") - F.col("Ventas_Valor_Original")) > 
                     F.abs(F.col("Ventas_Valor_Original") * 0.01))
                ).count()
                
                if discrepancias > 0:
                    logger.warning(f"Discrepancias en Ventas_Valor corregidas: {discrepancias:,}")
        
        # =====================================================================
        # PASO 7: MANEJAR CASOS ANÓMALOS
        # =====================================================================
        logger.info("\n--- PASO 7: Manejo de Registros Anómalos ---")
        
        anomalos_count = df.filter(F.col("Tipo_Transaccion") == "ANOMALO").count()
        logger.info(f"Registros anómalos identificados: {anomalos_count:,}")
        
        if anomalos_count > 0:
            logger.warning("Ejemplos de registros anómalos:")
            df.filter(F.col("Tipo_Transaccion") == "ANOMALO") \
              .select(
                  "Producto_ID", "Ventas_Volumen", "Precio_Original", "Precio",
                  "Ventas_Valor_Original", "Ventas_Valor", "Out_Of_Stock_Flag"
              ).show(10, truncate=False)
            
            # DECISIÓN: ¿Eliminar o mantener con flag?
            # RECOMENDACIÓN: MANTENER con flag para análisis posterior
            logger.info("Los registros anómalos se mantienen marcados con Es_Anomalo=True")
            logger.info("Recomendación: Revisar manualmente o excluir de análisis críticos")
        
        after_anomalos = df.count()
        
        # =====================================================================
        # CONTINUARÁ CON DISTRIBUCIÓN PONDERADA Y OUTLIERS...
        # =====================================================================
        
        # 2. Limpiar Precio
        precio_negativo = df.filter(F.col("Precio") <= 0).count()
        precio_null = df.filter(F.col("Precio").isNull()).count()
        
        logger.info(f"Precios <= 0: {precio_negativo:,}")
        logger.info(f"Precios nulos: {precio_null:,}")
        
        if remove_null_precio:
            df = df.filter(
                F.col("Precio").isNotNull() &
                (F.col("Precio") > 0)
            )
        
        # 3. Limpiar Ventas_Volumen
        volumen_null = df.filter(F.col("Ventas_Volumen").isNull()).count()
        logger.info(f"Volúmenes nulos: {volumen_null:,}")
        
        if remove_null_volumen:
            df = df.filter(F.col("Ventas_Volumen").isNotNull())
        
        # 4. Eliminar outliers en Ventas_Volumen (opcional)
        outliers_removed = 0
        if remove_outliers:
            stats = df.select(
                F.mean("Ventas_Volumen").alias("mean"),
                F.stddev("Ventas_Volumen").alias("std")
            ).collect()[0]
            
            mean_vol = stats['mean']
            std_vol = stats['std']
            
            lower_bound = mean_vol - (outlier_std_threshold * std_vol)
            upper_bound = mean_vol + (outlier_std_threshold * std_vol)
            
            outliers = df.filter(
                (F.col("Ventas_Volumen") < lower_bound) |
                (F.col("Ventas_Volumen") > upper_bound)
            ).count()
            
            logger.info(f"Outliers detectados: {outliers:,}")
            logger.info(f"  Umbral: {outlier_std_threshold} std ({lower_bound:.2f} - {upper_bound:.2f})")
            
            df = df.filter(
                (F.col("Ventas_Volumen") >= lower_bound) &
                (F.col("Ventas_Volumen") <= upper_bound)
            )
            outliers_removed = outliers
        
        # =====================================================================
        # PASO 8: IMPUTAR DISTRIBUCION_PONDERADA
        # =====================================================================
        logger.info("\n--- PASO 8: Imputar Distribución Ponderada ---")
        
        dist_null = df.filter(F.col("Distribucion_Ponderada").isNull()).count()
        logger.info(f"Distribucion_Ponderada nulos: {dist_null:,}")
        
        if impute_distribucion and dist_null > 0:
            # Calcular mediana
            median_dist = df.approxQuantile("Distribucion_Ponderada", [0.5], 0.01)[0]
            logger.info(f"  Imputando con mediana: {median_dist:.4f}")
            
            df = df.withColumn(
                "Distribucion_Ponderada",
                F.coalesce(F.col("Distribucion_Ponderada"), F.lit(median_dist))
            )
        
        # =====================================================================
        # PASO 9: ELIMINAR OUTLIERS (OPCIONAL)
        # =====================================================================
        logger.info("\n--- PASO 9: Detección y Eliminación de Outliers ---")
        df = df.filter(
            F.col("Tienda_ID").isNotNull() &
            F.col("Producto_ID").isNotNull() &
            F.col("Fecha_ID").isNotNull()
        )
        
        final_count = df.count()
        removed = initial_count - final_count
        
        logger.info(f"Registros finales: {final_count:,}")
        logger.info(f"Registros eliminados: {removed:,} ({removed/initial_count*100:.2f}%)")
        
        # Estadísticas finales por tipo de transacción
        logger.info("\nDistribución FINAL por tipo de transacción:")
        df.groupBy("Tipo_Transaccion").count().orderBy(F.col("count").desc()).show(truncate=False)
        
        # Guardar reporte
        self.cleaning_report['fact_ventas'] = {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed': removed,
            'fecha_id_invalidos': fecha_id_invalidos,
            'fecha_id_centinela': FECHA_CENTINELA if fecha_id_invalidos > 0 else None,
            'precio_negativo': precio_negativo,
            'precio_null': precio_null,
            'precio_imputados': precio_null + precio_invalido if precio_null > 0 or precio_invalido > 0 else 0,
            'volumen_null': volumen_null,
            'dist_ponderada_null': dist_null,
            'outliers_removed': outliers_removed,
            'anomalos_count': anomalos_count,
            'tipos_transaccion': {
                row['Tipo_Transaccion']: row['count'] 
                for row in df.groupBy("Tipo_Transaccion").count().collect()
            }
        }
        
        self.cleaned_dfs['fact_ventas'] = df
        return df
    
    # =========================================================================
    # INTEGRIDAD REFERENCIAL
    # =========================================================================
    
    def enforce_referential_integrity(self) -> Optional[DataFrame]:
        """
        Aplica integridad referencial eliminando registros huérfanos.
        
        Elimina registros de fact_ventas donde:
        - Fecha_ID no existe en dim_tiempo
        - Tienda_ID no existe en dim_tienda
        - Producto_ID no existe en dim_producto
        
        Returns:
            fact_ventas limpio o None si falla
        """
        logger.info("\n" + "="*60)
        logger.info("APLICANDO INTEGRIDAD REFERENCIAL")
        logger.info("="*60)
        
        fact = self.cleaned_dfs.get('fact_ventas')
        dim_tiempo = self.cleaned_dfs.get('dim_tiempo')
        dim_tienda = self.cleaned_dfs.get('dim_tienda')
        dim_producto = self.cleaned_dfs.get('dim_producto')
        
        if not all([fact, dim_tiempo, dim_tienda, dim_producto]):
            logger.error("No todas las tablas están limpias")
            return None
        
        initial_count = fact.count()
        
        # 1. Validar Fecha_ID
        valid_fechas = dim_tiempo.select("Fecha_ID").distinct()
        fact = fact.join(valid_fechas, "Fecha_ID", "inner")
        
        after_fecha = fact.count()
        removed_fecha = initial_count - after_fecha
        logger.info(f"Registros con Fecha_ID huérfano eliminados: {removed_fecha:,}")
        
        # 2. Validar Tienda_ID
        valid_tiendas = dim_tienda.select("Tienda_ID").distinct()
        fact = fact.join(valid_tiendas, "Tienda_ID", "inner")
        
        after_tienda = fact.count()
        removed_tienda = after_fecha - after_tienda
        logger.info(f"Registros con Tienda_ID huérfano eliminados: {removed_tienda:,}")
        
        # 3. Validar Producto_ID
        valid_productos = dim_producto.select("Producto_ID").distinct()
        fact = fact.join(valid_productos, "Producto_ID", "inner")
        
        final_count = fact.count()
        removed_producto = after_tienda - final_count
        logger.info(f"Registros con Producto_ID huérfano eliminados: {removed_producto:,}")
        
        total_removed = initial_count - final_count
        logger.info(f"\nTotal registros eliminados: {total_removed:,} ({total_removed/initial_count*100:.2f}%)")
        logger.info(f"Registros finales: {final_count:,}")
        
        # Actualizar reporte
        if 'fact_ventas' not in self.cleaning_report:
            self.cleaning_report['fact_ventas'] = {}
        
        self.cleaning_report['fact_ventas'].update({
            'referential_integrity': {
                'removed_fecha': removed_fecha,
                'removed_tienda': removed_tienda,
                'removed_producto': removed_producto,
                'total_removed': total_removed
            }
        })
        
        self.cleaned_dfs['fact_ventas'] = fact
        return fact

    # =========================================================================
    # MÉTODOS DE EJECUCIÓN
    # =========================================================================
    
    def clean_all(
        self,
        enforce_integrity: bool = True,
        fact_options: Optional[Dict] = None
    ) -> Dict[str, DataFrame]:
        """
        Ejecuta limpieza completa de todas las tablas.
        
        Args:
            enforce_integrity: Aplicar integridad referencial
            fact_options: Opciones para clean_fact_ventas()
            
        Returns:
            Diccionario con {table_name: cleaned_DataFrame}
        """
        logger.info("\n" + "="*70)
        logger.info(" "*20 + "INICIANDO LIMPIEZA COMPLETA")
        logger.info("="*70)
        
        # Opciones por defecto para fact_ventas
        if fact_options is None:
            fact_options = {
                'remove_null_precio': True,
                'remove_null_volumen': True,
                'impute_distribucion': True,
                'recalculate_valor': True,
                'remove_outliers': False
            }
        
        # Limpiar dimensiones primero
        self.clean_dim_tiempo()
        self.clean_dim_tienda()
        self.clean_dim_producto()
        
        # Limpiar tabla de hechos
        self.clean_fact_ventas(**fact_options)
        
        # Aplicar integridad referencial
        if enforce_integrity:
            self.enforce_referential_integrity()
        
        logger.info("\n" + "="*70)
        logger.info(" "*20 + "LIMPIEZA COMPLETADA")
        logger.info("="*70)
        
        return self.cleaned_dfs
    
    def get_cleaning_report(self) -> Dict:
        """
        Retorna el reporte completo de limpieza.
        
        Returns:
            Diccionario con estadísticas de limpieza por tabla
        """
        return self.cleaning_report
    
    def print_cleaning_summary(self) -> None:
        """
        Imprime un resumen legible del proceso de limpieza.
        """
        if not self.cleaning_report:
            logger.warning("No hay reporte de limpieza disponible")
            return
        
        print("\n" + "="*70)
        print(" "*20 + "RESUMEN DE LIMPIEZA")
        print("="*70)
        
        for table_name, report in self.cleaning_report.items():
            print(f"\n{table_name.upper()}")
            print("-" * 60)
            print(f"  Registros iniciales:  {report.get('initial_count', 0):>12,}")
            print(f"  Registros finales:    {report.get('final_count', 0):>12,}")
            print(f"  Registros eliminados: {report.get('removed', 0):>12,}")
            
            pct = (report.get('removed', 0) / report.get('initial_count', 1) * 100)
            print(f"  Porcentaje eliminado: {pct:>12.2f}%")
            
            # Detalles específicos
            if table_name == 'fact_ventas' and 'referential_integrity' in report:
                ref = report['referential_integrity']
                print(f"\n  Integridad Referencial:")
                print(f"    - Fecha_ID huérfanos:    {ref['removed_fecha']:>10,}")
                print(f"    - Tienda_ID huérfanos:   {ref['removed_tienda']:>10,}")
                print(f"    - Producto_ID huérfanos: {ref['removed_producto']:>10,}")
        
        print("\n" + "="*70)
    
    def get_cleaned_dataframe(self, table_name: str) -> Optional[DataFrame]:
        """
        Obtiene un DataFrame limpio.
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            DataFrame limpio o None si no existe
        """
        return self.cleaned_dfs.get(table_name)