# app.py
import os
from flask import Flask, request, render_template
from dotenv import load_dotenv

from rag_service import run_research, generate_research_plan

# Загружаем переменные окружения из .env
load_dotenv()

# Создаем экземпляр Flask приложения
# __name__ помогает Flask найти шаблоны и статические файлы
app = Flask(__name__)

# Секретный ключ для сессий Flask (не используется здесь, но хорошая практика)
# app.secret_key = os.getenv("FLASK_SECRET_KEY", "a_default_secret_key")

@app.route('/', methods=['GET', 'POST'])
def index():
    """Обрабатывает главную страницу (GET) и отправку формы (POST)."""
    topic_input = ""
    error = None
    editmode = False

    result = request.form.get('result', '')
    prompt = request.form.get('prompt', '')
    new_prompt = request.form.get('new-prompt', '')

    if request.method == 'POST':

        topic_input = request.form.get('topic', '').strip()

        if request.form.get('start-button'):
            if not topic_input:
                error = "Пожалуйста, введите тему для исследования."
            else:
                try:
                    print(f"[Flask App] Received topic: '{topic_input}'. Calling RAG service...")
                    prompt = generate_research_plan(topic_input) # функция которая генерирует промт
                    print(f"[Flask App] RAG service returned result.")
                except Exception as e:
                    print(f"[Flask App] Error during RAG execution: {e}")
                    error = f"Произошла ошибка при обработке запроса: {e}"

        if new_prompt:
            prompt = new_prompt
        print(prompt)
        if 'next-button' in request.form:
            result = run_research(topic_input, plan = prompt) # функция которая делает исследование по промту

        # обработка кнопки "редактировать"
        if 'edit-button' in request.form:
            editmode = True 
            
        if 'exit-button' in request.form:
            editmode = False

    return render_template(
        'index.html',
        topic_input=topic_input, # Чтобы поле ввода не очищалось
        result=result,           # Результат исследования (или None)
        error=error,             # Сообщение об ошибке (или None)
        prompt=prompt,           # Промт
        editmode=editmode        # Режим редактирования
    )

if __name__ == '__main__':
    # Запускаем встроенный сервер Flask для разработки
    # debug=True автоматически перезагружает сервер при изменении кода
    # host='0.0.0.0' делает сервер доступным из локальной сети (опционально)
    print("Starting Flask development server...")
    app.run(debug=True, host='0.0.0.0', port=5000)