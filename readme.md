Ejecutar el script:

python load.py

El script genera automáticamente una carpeta output/ con los archivos limpios.

Archivos generados
fact_otr.csv: tabla principal de órdenes de venta.
dim_clientes.csv: maestro de clientes limpio.
dim_productos.csv: maestro de productos limpio.
data_quality_log.csv: registro de problemas de calidad detectados.
otr_clean.db: base SQLite con las mismas tablas.
Modelo de datos

Se definió un modelo simple con una tabla principal de hechos y dos dimensiones:

fact_otr: contiene las órdenes de venta.
dim_clientes: contiene información comercial de clientes.
dim_productos: contiene información logística de productos.

Este diseño permite separar la información transaccional de los maestros, facilitando el análisis posterior en Power BI.

Limpieza realizada
SKU

Se estandarizó el campo SKU eliminando ceros a la izquierda para poder cruzar correctamente la hoja OTR con Paletizado.

Ejemplo:

0000010001 → 10001

Productos duplicados

Se detectaron productos duplicados en la hoja Paletizado. Para evitar duplicar registros al cruzar tablas, se dejó un solo registro por SKU.

Clientes

Se normalizaron los nombres de clientes usando la hoja Clientes como maestro. Se corrigieron diferencias de escritura, espacios y mayúsculas.

Ejemplos:

Jumbo → JUMBO
Tottus S.A. → TOTTUS
Lider Express → LIDER EXPRESS
Comidas Prep OOH → COMIDAS PREPARADAS OOH

Clientes sin maestro

Se detectaron clientes presentes en OTR que no existían en la hoja Clientes, como Distribuidora XYZ.

Estos registros no fueron eliminados. Se mantuvieron en la base y se agregaron a dim_clientes con:

CANAL = SIN MAESTRO
HOLDING = SIN MAESTRO
REGION = SIN MAESTRO

Cantidades negativas

Se detectaron cantidades negativas en OTR. Para efectos del análisis logístico, se transformaron a valor absoluto en la columna CANTIDAD.

Supuesto: si estos registros corresponden a devoluciones, ajustes comerciales o notas de crédito, deberían manejarse en un flujo separado y no dentro de la base de órdenes de despacho.

Fechas de entrega

Se creó la columna DIAS_DIFERENCIA_ENTREGA, calculada como:

FECHA_ENTREGA - FECHA_SOLICITADA_ENTREGA

Interpretación:

valor negativo = entrega anticipada
valor cero = entrega en la fecha solicitada
valor positivo = entrega posterior a la fecha solicitada

Los valores negativos no fueron corregidos, ya que representan entregas anticipadas.

Idempotencia

El script es idempotente porque cada ejecución reemplaza los archivos de salida y las tablas generadas. Por lo tanto, correr load.py más de una vez no duplica datos.

Resultado

El proceso genera una versión limpia y estructurada de la información, lista para ser conectada a Power BI en la siguiente etapa.