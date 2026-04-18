# Guion de Presentación: Proyecto ETL Automático - ICETEX & KPIs Macroeconómicos

**Objetivo del documento:** Este guion está diseñado para guiarte paso a paso durante la presentación de tu proyecto. El formato está dividido en dos partes por sección:
- 🎬 **LO QUE DEBES MOSTRAR:** Acciones en pantalla (qué archivo abrir, a qué pestaña ir, qué señalar).
- 🗣️ **LO QUE DEBES DECIR:** El guion exacto de tu exposición adaptable a tus propias palabras.

---

## 1. Introducción y Contexto (Lo que hicimos en la Entrega 1)

🎬 **LO QUE DEBES MOSTRAR:** 
* Abre tu presentación en la diapositiva del Título o abre el repositorio de forma general. 
* Muestra rápidamente (si lo tienes) un diagrama de la arquitectura del Data Warehouse inicial (Modelo Estrella).

🗣️ **LO QUE DEBES DECIR:**
> "Buenos días a todos. Hoy les presentaré la evolución de nuestro proyecto de Ingeniería de Datos basado en la asignación de créditos del ICETEX. 
> 
> Para contextualizar un poco: en la **primera entrega**, nuestro enfoque fue puramente de modelado y estructuración base. Tomamos un dataset estático de beneficiarios del ICETEX, definimos nuestro *nivel de granularidad* (un registro por año, departamento, modalidad, y estrato), y diseñamos un Modelo Estrella tradicional con un *Data Warehouse* centralizado. 
> Hasta ese punto, el proceso era manual y analítico. Extraíamos la data, la subíamos a una base y sacábamos métricas aisladas. Funcionaba bien, pero no era escalable ni automatizado para un entorno empresarial real."

---

## 2. Los Nuevos Objetivos (Segunda Entrega)

🎬 **LO QUE DEBES MOSTRAR:** 
* Muestra el Diagrama de Bloques General del Proyecto (Figura 1 en tu PDF de requerimientos o el diagrama que tengas) donde se vea la API, Airflow, Great Expectations y Postgres.

🗣️ **LO QUE DEBES DECIR:**
> "En esta **segunda entrega**, nuestro objetivo es transformar esa solución analítica manual en un **Pipeline ETL automatizado, resiliente y de grado productivo.**
> 
> Para lograrlo, nos enfocamos en 3 grandes pilares tecnológicos:
> 1. **Extender los Datos:** Dejamos de ver el Icetex como un elemento aislado, y nos conectamos mediante API a *Socrata* para consumir Opendata gubernamental. Específicamente trajimos el **PIB Departamental del DANE**. Queremos saber si la asignación de créditos responde a la riqueza o pobreza de un departamento.
> 2. **Orquestación Total:** Evitamos los scripts sueltos y orquestamos cada paso en un clúster *Apache Airflow*.
> 3. **Observabilidad y Calidad:** Posiblemente el paso más crítico; implementamos puertas de calidad o *Quality Gates* usando *Great Expectations* para que el pipeline nos defienda proactivamente de datos corruptos antes de que lleguen a las visualizaciones."

---

## 3. Orquestación Automatizada con Apache Airflow

🎬 **LO QUE DEBES MOSTRAR:** 
* Abre y muestra tu navegador web en la interfaz de **Apache Airflow (`localhost:8081`)** en la vista de *Graph* o *Grid*.
* **Si es posible**, haz clic en una ejecución exitosa (verde) para mostrar todas las cajitas de los "Tasks" en verde conectadas por flechas.
* Abre tu editor de código y muestra brevemente el archivo `icetex_etl_dag.py`.

🗣️ **LO QUE DEBES DECIR:**
> "¿Por qué usar Airflow? En un proyecto real, las fuentes de datos se rompen, las redes fallan y los procesos cambian. Necesitamos un 'director de orquesta' que decida qué ejecutar, en qué orden y cómo reaccionar ante errores.
> 
> Decidimos crear un DAG (Grafo Acíclico Dirigido) altamente modular:
> * Al inicio, tenemos dos ramas ejecutándose en paralelo: Extraemos el histórico del ICETEX de manera local, y simultáneamente usamos la librería *Sodapy* para hacer solicitudes limpias y paginadas a la API de Socrata extrayendo el PIB.
> * Una vez que ambas fuentes finalizan (apuntando al principio de dependencia estricta), procedemos a la tarea de Transformación y Cruce (`transform_and_merge`). Aquí tomamos dos datasets que vienen escritos distinto ('BOGOTÁ, D.C.' vs 'Bogota'), y los resolvemos matemáticamente usando lógica *Fuzzy String Matching* para alinear las métricas a un mismo departamento y periodo de tiempo.
>
> *(Muestras el código en Airflow rápidamente)* 
> Usamos la API de TaskFlow de Airflow (`@task`), lo que hace el código muy limpio y nos permite pasar referencias de archivos pesados en disco en lugar de pasar la memoria viva (lo cual crashearía los workers). Todo está contenido en una infraestructura de Docker Compose."

---

## 4. Validaciones de Datos con Great Expectations (EL CORAZÓN DE LA ENTREGA)

🎬 **LO QUE DEBES MOSTRAR:** 
* Haz énfasis importante acá. En tu editor, abre el archivo `src/setup_gx.py` o navega por los logs en Airflow apuntando a la tarea `run_quality_checks`.
* Muestra el archivo HTML de resultados: *Data Docs* (ubicado en `gx/uncommitted/data_docs/local_site/index.html`). Ábrelo en el navegador si puedes; es la mejor forma de explicar qué se validó.

🗣️ **LO QUE DEBES DECIR:**
> "Quiero detener especial atención a nuestra Tarea 3: La puerta de validación con **Great Expectations**. 
> Es inaceptable en análisis de datos asumir que la API externa siempre envía la información bien escrita. Una falla aquí contaminaría el Data Warehouse y los Dashboards gerenciales.
> 
> *Great Expectations* funciona como un test unitario, pero para los datos en crudo. Implementamos nuestro *Data Context* de forma programática. Decidimos construir nuestras propias *'Expectations'* (expectativas) o reglas de negocio antes de inyectar nada en PostgreSQL.
> 
> Dividimos nuestras reglas en validaciones Críticas y Flexibles:
> 
> 1. **Reglas de Integridad (Críticas):** Validamos que la columna `VIGENCIA` (el año) y el `CÓDIGO DE DEPARTAMENTO` **jamás sean nulos** (`expect_column_values_to_not_be_null`). Esto es innegociable. Si falta un año, las Foráneas del Modelo Estrella se rompen y la base colapsa.
> 2. **Reglas de Dominio Lógico (Críticas):** Comprobamos el negocio real. Añadimos la función `expect_column_values_to_be_in_set` para la columna *Sector IES*. Exigimos agresivamente que diga únicamente 'OFICIAL', 'PRIVADO' o 'NO CLASIFICADO'. Un solo registro que diga 'privada' en minúscula y el pipeline se tumba, e impide ver un Sector duplicado.
> 3. **Reglas de Estratos:** Tenemos una alerta donde el grupo objetivo (Estrato 1 a 6) es validado en el set.
> 4. **Reglas de Volumen y Rangos:** Agregamos expectativas numéricas para pedir que no se den beneficiarios negativos en el Icetex, y validamos la forma final del dataframe para garantizar que el cruce (JOIN) no creara explosiones de datos masivas. El tamaño permitido es restricto a entre 50.000 y 150.000 filas.
>
> *(Muestras el Airflow)*
> Como Control de Mando, si cualquiera de las reglas crìticas falla durante la ejecución de esa tarea en particular (`run_quality_checks`), la tarea pasa a `FAILED` y el pipeline **frena el paso hacia Postgres**. 
> Como vemos acá hoy, nuestro pipeline logra estabilizar sobre 107.500 filas perfectas y seguras."

---

## 5. Definición de KPIs antes del Dashboard Analytics

🎬 **LO QUE DEBES MOSTRAR:** 
* Cambia la pantalla a tu editor de postgres (ej. DBeaver, pgAdmin) o al archivo de queries SQL. Alternativamente, si tienes un slide, pon únicamente las fórmulas de los KPIs.

🗣️ **LO QUE DEBES DECIR:**
> "Con la data aprobada e insertada en el Data Warehouse, creamos en el motor PostgreSQL una **Vista Materializada Analítica** (`mv_impacto_macro_desplazamiento`). 
> Queríamos resolver visualmente el efecto del PIB frente a la solicitud de créditos, calculando nativamente estos KPIs por medio de métodos matemáticos de base de datos como las Window Functions:
> 
> 1. **Variación interanual (YoY) de Beneficiarios:** Permite ver el porcentaje histórico de crecimientos de un año al otro por cada departamento y sector.
> 2. **Share o Cuota de Sector Mensurable:** Qué porcentaje del mercado domina la universidad Privada versus la Oficial en un territorio dado.
> 3. **Rankin Regional:** Posicionamiento jerárquico de los departamentos que más masa de estudiantes manejan en todo el territorio del país.
> 4. **Métrica Combinada Macro:** Finalmente, cómo se correlaciona el PIB extraído dinámicamente desde la API (valor en Miles de Millones) frente al esfuerzo que hacen por solicitar un crédito del ICETEX en ese lugar de origen."

---

## 6. Dashboards, Visualizaciones e Insights Reales

🎬 **LO QUE DEBES MOSTRAR:** 
* Abre tu herramienta de tableros (*Power BI / Tableau / Looker*). 
* Navega por los menús interactivos donde tienes gráficamente expuestos tus resultados finales. Mueve un par de filtros para demostrar conexiones en tiempo real. 

🗣️ **LO QUE DEBES DECIR:**
> "Para finalizar y obtener beneficios tangibles del Dato, conectamos nuestro tablado BI, directamente al DW alimentado por el cron de Airflow.
> 
> *(Muestras las Gráficas interactivas y señalas descubrimientos)*
> Como se puede observar en nuestras visualizaciones *(acá mencionas una cosa interesante)*: 
> 'Identificamos que aun cuando ciertos departamentos sufren una caída o desaceleración en su PIB (producto que trajimos desde la API), existe un efecto colateral donde la demanda de ICETEX de la modalidad pública experimenta cambios en Estratos 1 y 2'.
> 
> El valor integral del proyecto que les presento hoy no estriba solamente en un esquema visual, sino en algo en particular detrás: **Data Ops**. Gracias a la capacidad reactiva del Apache Airflow, el cruce georeferencial automático con APIs gubernamentales, y las rigurosas validaciones *Test-Driven* de Great Expectations, estos datos presentados no son solo teóricos. Conforman una arquitectura real **altamente escalable e incuestionable en su precisión.**
>
> Les agradezco el tiempo y con total gusto abro a preguntas sobre la ingeniería, infraestructura o los resultados analíticos presentados."

---

## 7. Consejos de oro para salir invicto ante dudas o trabas:
1. **Evita fallas en vivo:** Ten todo Docker Compose corriendo, y el pipeline y PowerBI listos y procesados previamente. Si te piden darle ejecutar en vivo por sorpresa a Airflow, recuerda tener PostgreSQL limpio de conexiones trabadas que bloqueen los tasks, aunque siempre las tasks en verde (exitosas) sirven de sustento total.
2. Si un evaluador te cuestiona **"¿Qué es específicamente lo que la API mejora al modelo?"**: Responde, *"Nos saca del vacío informativo. Añade el KPI del Producto Interno Bruto (el macroindicador número 1 en desarrollo de Colombia). Conocer solo cuánto dinero presta ICETEX era bidimensional. Conocer la relación que el volumen prestado tiene correlacionado al rendimiento económico interno de CADA departamento es un análisis completo"*
3. Si el evaluador indaga **"¿Y qué pasa si hay un fallo y queremos sumar otra base en vez del PIB de DANE?"**, contesta contundentemente: *"La modularidad nos favorece. La tarea está decorada en Python como un nodo extraíble en Airflow. En el peor caso construimos un nuevo Task y una nueva Expectación en nuestra suite (Great Expectations) reajustando la regla del negocio; el transformador adaptaría y en ningún momento el visualizador ni la tabla original tendrían de qué preocuparse debido al soft-fail model de nuestro Data Pipeline"*.
