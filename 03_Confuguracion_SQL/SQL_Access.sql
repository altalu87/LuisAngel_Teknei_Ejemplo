-- ============================================
-- 5. ASIGNACIÓN DE PERMISOS AL USUARIO pySparkUser
-- ============================================
-- Se otorgan permisos para que el usuario pueda crear tablas, insertar, actualizar, eliminar y consultar datos.
GRANT CREATE TABLE TO [pySparkUser];
GRANT INSERT, UPDATE, DELETE ON DATABASE::DB_Beverages TO [pySparkUser];
GRANT SELECT ON SCHEMA::dbo TO [pySparkUser];
GO