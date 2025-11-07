-- ============================================
-- 4. CREACIÓN DE PROCEDIMIENTO ALMACENADO PARA VALIDACIÓN E INSERCIÓN
-- ============================================
-- Este procedimiento:
-- - Valida si los registros en las tablas temporales ya existen en las tablas finales.
-- - Inserta solo los registros nuevos.
-- - Registra el resultado en la tabla de log.
-- - Elimina los datos procesados de las tablas temporales.
-- - Procesa clean_fact_ventas en lotes para evitar truncamientos.
USe DB_Beverages
go

--CREATE PROCEDURE dbo.sp_validar_e_insertar_etl
ALTER PROCEDURE dbo.sp_validar_e_insertar_etl
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @insertados INT;
    DECLARE @existentes INT;

    -- clean_dim_producto → tbl_producto
    INSERT INTO dbo.tbl_producto (Producto_ID, Marca, Segmento, Sabor, Tipo_Envase, Tamano, Precio_Base, Demanda_Factor)
    SELECT c.Producto_ID, c.Marca, c.Segmento, c.Sabor, c.Tipo_Envase, c.Tamano, c.Precio_Base, c.Demanda_Factor
    FROM dbo.clean_dim_producto c
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.tbl_producto p WHERE p.Producto_ID = c.Producto_ID
    );

    SET @insertados = @@ROWCOUNT;

    SELECT @existentes = COUNT(*) FROM dbo.clean_dim_producto c
    WHERE EXISTS (
        SELECT 1 FROM dbo.tbl_producto p WHERE p.Producto_ID = c.Producto_ID
    );

    INSERT INTO dbo.etl_log (Tabla_Origen, Tabla_Destino, Registros_Insertados, Registros_Existentes, Observaciones)
    VALUES ('clean_dim_producto', 'tbl_producto', @insertados, @existentes, 'Inserción desde ETL');

    DELETE FROM dbo.clean_dim_producto;

    -- clean_dim_tienda → tbl_tienda
    INSERT INTO dbo.tbl_tienda (Tienda_ID, Region, Formato_Tienda, Atractivo_Factor, Region_original)
    SELECT c.Tienda_ID, c.Region, c.Formato_Tienda, c.Atractivo_Factor, c.Region_original
    FROM dbo.clean_dim_tienda c
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.tbl_tienda t WHERE t.Tienda_ID = c.Tienda_ID
    );

    SET @insertados = @@ROWCOUNT;

    SELECT @existentes = COUNT(*) FROM dbo.clean_dim_tienda c
    WHERE EXISTS (
        SELECT 1 FROM dbo.tbl_tienda t WHERE t.Tienda_ID = c.Tienda_ID
    );

    INSERT INTO dbo.etl_log (Tabla_Origen, Tabla_Destino, Registros_Insertados, Registros_Existentes, Observaciones)
    VALUES ('clean_dim_tienda', 'tbl_tienda', @insertados, @existentes, 'Inserción desde ETL');

    DELETE FROM dbo.clean_dim_tienda;

    -- clean_dim_tiempo → tbl_fecha
    INSERT INTO dbo.tbl_fecha (Fecha_ID, Fecha, Anio, Mes, Nombre_Mes, Numero_Semana, Trimestre, Estacionalidad_Factor)
    SELECT c.Fecha_ID, c.Fecha, c.Anio, c.Mes, c.Nombre_Mes, c.Numero_Semana, c.Trimestre, c.Estacionalidad_Factor
    FROM dbo.clean_dim_tiempo c
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.tbl_fecha f WHERE f.Fecha_ID = c.Fecha_ID
    );

    SET @insertados = @@ROWCOUNT;

    SELECT @existentes = COUNT(*) FROM dbo.clean_dim_tiempo c
    WHERE EXISTS (
        SELECT 1 FROM dbo.tbl_fecha f WHERE f.Fecha_ID = c.Fecha_ID
    );

    INSERT INTO dbo.etl_log (Tabla_Origen, Tabla_Destino, Registros_Insertados, Registros_Existentes, Observaciones)
    VALUES ('clean_dim_tiempo', 'tbl_fecha', @insertados, @existentes, 'Inserción desde ETL');

    DELETE FROM dbo.clean_dim_tiempo;

    -- clean_fact_ventas → tbl_facts (por lotes)
    DECLARE @batch_size INT = 50000;
    DECLARE @total_inserted INT = 0;
    DECLARE @batch_inserted INT;

	WHILE EXISTS (SELECT 1 FROM dbo.clean_fact_ventas)
	BEGIN
		-- Insertar lote
		INSERT INTO dbo.tbl_facts (
			Producto_ID, Tienda_ID, Fecha_ID, Distribucion_Numerica, Distribucion_Ponderada,
			Precio, Out_Of_Stock_Flag, Ventas_Volumen, Ventas_Valor, Fecha_ID_original,
			Precio_Promedio_Producto, Tipo_Transaccion, Es_Devolucion, Es_Ajuste,
			Es_Venta_Normal, Es_Anomalo, Precio_Original, Ventas_Valor_Original
		)
		SELECT TOP (50000) *
		FROM dbo.clean_fact_ventas c
		WHERE NOT EXISTS (
			SELECT 1 FROM dbo.tbl_facts f
			WHERE f.Producto_ID = c.Producto_ID
			  AND f.Tienda_ID = c.Tienda_ID
			  AND f.Fecha_ID = c.Fecha_ID
			  AND f.Tipo_Transaccion = c.Tipo_Transaccion
		);

		SET @batch_inserted = @@ROWCOUNT;
		SET @total_inserted += @batch_inserted;

		-- Eliminar lote procesado
		DELETE FROM dbo.clean_fact_ventas
		WHERE EXISTS (
			SELECT 1 FROM dbo.tbl_facts f
			WHERE dbo.clean_fact_ventas.Producto_ID = f.Producto_ID
			  AND dbo.clean_fact_ventas.Tienda_ID = f.Tienda_ID
			  AND dbo.clean_fact_ventas.Fecha_ID = f.Fecha_ID
			  AND dbo.clean_fact_ventas.Tipo_Transaccion = f.Tipo_Transaccion
		);
	END;

    INSERT INTO dbo.etl_log (Tabla_Origen, Tabla_Destino, Registros_Insertados, Registros_Existentes, Observaciones)
    VALUES ('clean_fact_ventas', 'tbl_facts', @total_inserted, NULL, 'Inserción por lotes desde ETL');
END;
GO