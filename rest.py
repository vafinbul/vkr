from flask import Flask, request, jsonify
import psycopg2
import pickle
import pandas as pd
import datetime
from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint



app = Flask(__name__)

@app.route("/swagger.json")
def create_swagger_spec():
    # Генерируем спецификацию Swagger на основе документации вашего приложения Flask
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "PostgreSQL prewarming tool"
    swag['paths']['/prewarm'] = {
        'post': {
            'summary': 'Prewarm the PostgreSQL cache',
            'description': 'Endpoint for prewarming the PostgreSQL cache',
            'parameters': [
                {
                    'name': 'timestamp',
                    'in': 'query',
                    'description': 'Optional timestamp parameter',
                    'required': False,
                    'schema': {
                        'type': 'string',
                        'format': 'date-time'
                    }
                }
            ],
            'responses': {
                '200': {
                    'description': 'Success'
                },
                '400': {
                    'description': 'Bad request'
                }
            }
        }
    }

    return jsonify(swag)

SWAGGER_URL = '/api/docs'  # URL для Swagger UI
API_URL = '/swagger.json'  # URL для спецификации Swagger JSON

# Создаем объект Swagger UI Blueprint
swagger_ui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "PostgreSQL prewarm"
    }
)

# Регистрируем Blueprint
app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)



@app.route('/prewarm', methods=['POST'])
def prewarm():
    conn = psycopg2.connect(
        dbname="snapshot",
        user="postgres",
        password="12345678",
        host="127.0.0.1",
        port="5433"
    )

    cur = conn.cursor()

    timestamp = request.json.get('timestamp', None)
    flag = True
    if timestamp and datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") <= datetime.datetime.now():
        cur.execute(
            "SELECT relname, timestamp, buffer_count FROM buffercache where buffer_count > 10 ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - TIMESTAMP %s ))) asc , buffer_count desc limit 10;",
            (timestamp,))
        fetch = cur.fetchall()
        records = set()
        for f in fetch:
            records.add(f[0])
    else:
        flag = False
        # Загружаем модель машинного обучения
        model = pickle.load(open("rf_model.pkl", 'rb'))
        print("OK")
        # Забираем последние записи из таблицы prediction
        cur.execute("SELECT relname, relname_encoded, now() FROM prediction ORDER BY timestamp DESC LIMIT 4;")

        # Создаем pandas dataframe
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['relname', 'relname_encoded', 'timestamp'])
        df['timestamp'] = df['timestamp'].apply(lambda x: x.timestamp()).values.reshape(-1, 1)

        # Делаем предсказание по каждой relname_encoded
        X = df[['relname_encoded', 'timestamp']]
        df['prediction'] = model.predict(X)

        # Сортируем по убыванию предсказания
        df.sort_values(by='prediction', ascending=False, inplace=True)

        records = df['relname'].values.tolist()
        print(records)
    conn_target = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345678",
        host="127.0.0.1",
        port="5435"
    )

    cur_target = conn_target.cursor()
    for record in records:
        cur_target.execute("SELECT pg_prewarm(%s)", (record,))
        conn_target.commit()

    conn_target.close()
    conn.close()

    if flag:
        return "Найдены предыдущие снимки данных. Начинаем восстановление данных", 200
    else:
        return "Не найдены снимки данных за эту дату. Предсказываем нагрузку и восстанавливаем данные", 200



if __name__ == '__main__':
    app.run(debug=True)
