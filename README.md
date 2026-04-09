# Paquete

Aplicacion web local para consultar cobertura por codigo postal usando una base SQLite generada a partir de un archivo Excel.

## Incluye

- Base SQLite por defecto en [data/current_database.db](C:\Users\Java\Documents\New%20project\Paquete\data\current_database.db)
- Excel de origen en [data/current_database.xlsx](C:\Users\Java\Documents\New%20project\Paquete\data\current_database.xlsx)
- Importacion de nuevos datos desde `.xlsx`
- Avisos cuando se detectan codigos postales repetidos
- Seleccion de sucursal origen:
  - Guadalajara (`GDL02/GDL`)
  - Culiacan (`CUL01/CUL`)
  - Los Mochis (`LMM01/LMM`)
  - Monterrey (`MTY02/MTY`)
- Busqueda por codigo postal
- Validacion de distancia menor o igual a `1600 km`

## Como ejecutar

```powershell
python app.py
```

Luego abre [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Estructura

- [app.py](C:\Users\Java\Documents\New%20project\Paquete\app.py): servidor local y API
- [coverage_data.py](C:\Users\Java\Documents\New%20project\Paquete\coverage_data.py): importacion del Excel y acceso a SQLite
- [static/index.html](C:\Users\Java\Documents\New%20project\Paquete\static\index.html): interfaz principal
- [static/styles.css](C:\Users\Java\Documents\New%20project\Paquete\static\styles.css): estilo visual
- [static/app.js](C:\Users\Java\Documents\New%20project\Paquete\static\app.js): comportamiento del frontend
