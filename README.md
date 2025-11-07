📊 Dashboard de Control de Riesgo y Ineficiencia Operacional (Beverages_Report.pbix)

Introducción

Este proyecto fue desarrollado para ejemplificar un caso de diagnostico frente a una problematica operativa identificada en la data de ventas.
El hallazgo central es una Tasa de Impacto Operativo del 200.4%, lo que implica que el costo de la ineficiencia operacional (Devoluciones, Ajustes, Anómalos) supera en más del doble a los ingresos generados por la Venta Bruta Limpia, resultando en una rentabilidad neta negativa o severamente comprometida.

1.	Objetivo del Dashboard El objetivo principal es transformar el dato de Venta Bruta a Costo de Fricción Operacional para:

-Cuantificar la Crisis: Establecer la magnitud real de la pérdida de valor.
-Diagnosticar la Causa: Aislar si la fuga se debe a fallas de Calidad (Devoluciones) o Proceso Interno (Ajustes de Inventario).
-Priorizar la Intervención: Generar un mapa de riesgo que indique dónde se deben enfocar los equipos de auditoría, logística y calidad para detener la hemorragia de capital.

2.	Estructura del Análisis.

📌 Pestaña 1: Resumen Ejecutivo y Rendimiento Histórico
Propósito: Establecer la narrativa de la crisis.

📌 Pestaña 2: Diagnóstico - Fugas de Valor y Riesgo Estratégico
Propósito: Identificar y priorizar a los culpables (Tiendas y Productos).

-Estructura del proyecto:

En cada una de las carpetas podrás encontrar cada uno de los archivos que se utilizaron para generar el proyecto de ejemplo.

-01_Generador_de_datos:  Generador del conjunto de datos sintéticos para el proyecto de ejemplo (detalle dentro del archivo Jupyter).
-02_Limpieza : Módulo de limpieza y transformación de datos. Incluye clases para lectura, limpieza y exportación desde Jupyter Notebook.
-03_Configuración : Gestión de conexión y parámetros de Base de Datos SQL Server.
-04_Reporte: Reporte en Power BI con las pestañas de Resumen ejecutivo y diagnóstico.


