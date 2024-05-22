import requests
import time
from pynput import mouse

HOST = 'http://127.0.0.1:8124'

query_ddl = """
create table mouse_movements(
x Int16,
y Int16,
deltaX Int16,
deltaY Int16,
clientTimeStamp Float32,
button Int8,
target String
) ENGINE = MergeTree()
ORDER BY (clientTimeStamp);
"""


def query(q, host=HOST, conn_timeout=1500, **kwargs):
    r = requests.post(host, data=q, params=kwargs, timeout=conn_timeout)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.text)


class MouseRecorder:
    def __init__(self, client):
        self.client = client
        self.last_x, self.last_y = 0, 0

    def on_move(self, x, y):
        deltaX = x - self.last_x
        deltaY = y - self.last_y
        self.last_x, self.last_y = x, y
        client_time_stamp = time.time()
        button = -1  # No button pressed
        target = "some_window"  # Замените на реальную логику определения окна

        query_insert = f"""
        INSERT INTO mouse_movements (x, y, deltaX, deltaY, clientTimeStamp, button, target) 
        VALUES ({x}, {y}, {deltaX}, {deltaY}, {client_time_stamp}, {button}, '{target}')
        """
        self.client(query_insert)

    def on_click(self, x, y, button, pressed):
        if pressed:
            client_time_stamp = time.time()
            deltaX = x - self.last_x
            deltaY = y - self.last_y
            self.last_x, self.last_y = x, y
            target = "some_window"  # Замените на реальную логику определения окна

            query_insert = f"""
            INSERT INTO mouse_movements (x, y, deltaX, deltaY, clientTimeStamp, button, target) 
            VALUES ({x}, {y}, {deltaX}, {deltaY}, {client_time_stamp}, {button.value}, '{target}')
            """
            self.client(query_insert)


def create_table():
    data = query(query_ddl)
    print(data)


def get_total_movements():
    query_select = "SELECT COUNT(*) FROM mouse_movements"
    data = query(query_select)
    print("Total movements:", data)


def get_movements_in_range():
    query_select = """
    SELECT target, COUNT(*) FROM mouse_movements 
    WHERE x < 1000 AND y < 1000 
    GROUP BY target
    """
    data = query(query_select)
    print("Movements in range x < 1000 AND y < 1000 grouped by target:\n", data)


def get_largest_movements():
    query_select = """
    SELECT x, y, deltaX, deltaY, target, (ABS(deltaX) + ABS(deltaY)) AS movement_size 
    FROM mouse_movements 
    ORDER BY movement_size DESC 
    LIMIT 10
    """
    data = query(query_select)
    print("Largest movements:\n", data)


if __name__ == '__main__':
    create_table()

    mouse_recorder = MouseRecorder(query)

    # Запуск отслеживания движений мыши
    with mouse.Listener(on_move=mouse_recorder.on_move, on_click=mouse_recorder.on_click) as listener:
        try:
            listener.join()
        except KeyboardInterrupt:
            # Выполнение запросов для анализа данных
            get_total_movements()
            get_movements_in_range()
            get_largest_movements()