USE DB_Beverages;
GO

-- ==========================================================================================
-- DOCUMENTACIÓN DEL PROCESO ETL: Creación de Base de Datos, Tablas, Procedimiento y Permisos
-- ==========================================================================================

-- ============================================
-- 1. CREACIÓN DE LA BASE DE DATOS
-- ============================================
-- Se crea la base de datos principal para almacenar la información del proceso ETL de bebidas.
CREATE DATABASE DB_Beverages;
GO

-- Seleccionamos la base de datos para trabajar
USE DB_Beverages;
GO

-- ============================================
-- 2. CREACIÓN DE TABLAS BASE DEL MODELO ESTRELLA
-- ============================================

-- Tabla de productos
CREATE TABLE dbo.tbl_producto (
    Producto_ID INT NULL,
    Marca NVARCHAR(100) NULL,
    Segmento NVARCHAR(100) NULL,
    Sabor NVARCHAR(100) NULL,
    Tipo_Envase NVARCHAR(100) NULL,
    Tamano NVARCHAR(50) NULL,
    Precio_Base FLOAT NULL,
    Demanda_Factor FLOAT NULL
);
GO

-- Tabla de tiendas
CREATE TABLE dbo.tbl_tienda (
    Tienda_ID INT NULL,
    Region NVARCHAR(100) NULL,
    Formato_Tienda NVARCHAR(50) NULL,
    Atractivo_Factor FLOAT NULL,
    Region_original NVARCHAR(100) NULL
);
GO

-- Tabla de fechas
CREATE TABLE dbo.tbl_fecha (
    Fecha_ID NVARCHAR(50) NULL,
    Fecha DATE NULL,
    Anio INT NULL,
    Mes INT NULL,
    Nombre_Mes NVARCHAR(50) NULL,
    Numero_Semana INT NULL,
    Trimestre NVARCHAR(10) NULL,
    Estacionalidad_Factor FLOAT NULL
);
GO

-- Tabla de hechos (ventas)
CREATE TABLE dbo.tbl_facts (
    Producto_ID INT NULL,
    Tienda_ID INT NULL,
    Fecha_ID NVARCHAR(50) NULL,
    Distribucion_Numerica FLOAT NULL,
    Distribucion_Ponderada FLOAT NULL,
    Precio FLOAT NOT NULL,
    Out_Of_Stock_Flag INT NULL,
    Ventas_Volumen INT NULL,
    Ventas_Valor FLOAT NULL,
    Fecha_ID_original NVARCHAR(50) NULL,
    Precio_Promedio_Producto FLOAT NOT NULL,
    Tipo_Transaccion NVARCHAR(50) NOT NULL,
    Es_Devolucion BIT NOT NULL,
    Es_Ajuste BIT NOT NULL,
    Es_Venta_Normal BIT NOT NULL,
    Es_Anomalo BIT NOT NULL,
    Precio_Original FLOAT NULL,
    Ventas_Valor_Original FLOAT NULL
);
GO

-- ============================================
-- 3. CREACIÓN DE TABLA DE LOG PARA AUDITORÍA DEL ETL
-- ============================================
-- Esta tabla almacena información sobre cada ejecución del proceso ETL:
-- tabla origen, tabla destino, fecha/hora, registros insertados, registros existentes, observaciones.
CREATE TABLE dbo.etl_log (
    Log_ID INT IDENTITY(1,1) PRIMARY KEY,
    Tabla_Origen NVARCHAR(100),
    Tabla_Destino NVARCHAR(100),
    Fecha_Ejecucion DATETIME DEFAULT GETDATE(),
    Registros_Insertados INT,
    Registros_Existentes INT,
    Observaciones NVARCHAR(500)
);
GO

-- ============================================================================
-- PASO 1: CREAR TABLA DE LOG (SI NO EXISTE)
-- ============================================================================

-- Tabla principal de log de ejecuciones
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_log')
BEGIN
    CREATE TABLE dbo.etl_log (
        Log_ID INT IDENTITY(1,1) PRIMARY KEY,
        Ejecucion_ID UNIQUEIDENTIFIER DEFAULT NEWID(),
        Tabla_Origen VARCHAR(100),
        Tabla_Destino VARCHAR(100),
        Registros_Procesados INT DEFAULT 0,
        Registros_Insertados INT DEFAULT 0,
        Registros_Duplicados INT DEFAULT 0,
        Registros_Errores INT DEFAULT 0,
        Duracion_Segundos DECIMAL(10,2),
        Estado VARCHAR(20), -- 'EXITOSO', 'ERROR', 'ADVERTENCIA'
        Mensaje VARCHAR(MAX),
        Usuario VARCHAR(100) DEFAULT SYSTEM_USER,
        Fecha_Inicio DATETIME2 DEFAULT GETDATE(),
        Fecha_Fin DATETIME2,
        CONSTRAINT CHK_etl_log_Estado CHECK (Estado IN ('EXITOSO', 'ERROR', 'ADVERTENCIA', 'EN_PROCESO'))
    );
    
    CREATE INDEX IX_etl_log_Ejecucion ON dbo.etl_log(Ejecucion_ID);
    CREATE INDEX IX_etl_log_Fecha ON dbo.etl_log(Fecha_Inicio DESC);
    
    PRINT '✓ Tabla etl_log creada';
END
GO

-- Tabla de errores detallados
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_error_detalle')
BEGIN
    CREATE TABLE dbo.etl_error_detalle (
        Error_ID INT IDENTITY(1,1) PRIMARY KEY,
        Ejecucion_ID UNIQUEIDENTIFIER,
        Tabla VARCHAR(100),
        Tipo_Error VARCHAR(50), -- 'DUPLICADO', 'INTEGRIDAD', 'VALIDACION', 'SISTEMA'
        Descripcion VARCHAR(MAX),
        Registros_Afectados INT,
        Datos_Ejemplo VARCHAR(MAX), -- JSON o texto con ejemplos
        Fecha_Error DATETIME2 DEFAULT GETDATE()
    );
    
    CREATE INDEX IX_error_Ejecucion ON dbo.etl_error_detalle(Ejecucion_ID);
    
    PRINT '✓ Tabla etl_error_detalle creada';
END
GO


/*ALTER TABLE dbo.clean_dim_tiempo
ALTER COLUMN Fecha_ID NVARCHAR(50);
ALTER TABLE dbo.clean_fact_ventas
ALTER COLUMN Fecha_ID NVARCHAR(50);
ALTER TABLE dbo.clean_fact_ventas
ALTER COLUMN Tipo_Transaccion NVARCHAR(50);
*/

-- ============================================================================
-- PASO 2: CREAR ÍNDICES EN TABLAS STAGING (OPTIMIZACIÓN)
-- ============================================================================

-- Estos índices son CRÍTICOS para el rendimiento del NOT EXISTS
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clean_dim_producto_PK')
    CREATE INDEX IX_clean_dim_producto_PK ON dbo.clean_dim_producto(Producto_ID);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clean_dim_tienda_PK')
    CREATE INDEX IX_clean_dim_tienda_PK ON dbo.clean_dim_tienda(Tienda_ID);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clean_dim_tiempo_PK')
    CREATE INDEX IX_clean_dim_tiempo_PK ON dbo.clean_dim_tiempo(Fecha_ID);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_clean_fact_ventas_FK')
    CREATE INDEX IX_clean_fact_ventas_FK ON dbo.clean_fact_ventas(
        Producto_ID, Tienda_ID, Fecha_ID, Tipo_Transaccion
    );

PRINT '✓ Índices en tablas staging verificados';
GO
