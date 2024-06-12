from flask import Flask, render_template
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def home():
    # Ruta relativa al archivo CSV
    csv_file_path = os.path.join(os.path.dirname(__file__), '..', 'valora_news.csv')

    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        return "Archivo CSV no encontrado.", 404

    # Convertir DataFrame a lista de diccionarios
    news = df.to_dict(orient='records')

    return render_template('index.html', news=news)

if __name__ == '__main__':
    app.run(debug=True)


