FROM python:3.10.3-slim-buster

# Add /app to the Python path
ENV PYTHONPATH=/app

# Stream output to console
ENV PYTHONUNBUFFERED=1

RUN pip install poetry==1.8.2

WORKDIR /app

# Copy poetry.lock and pyproject.toml files into container
COPY poetry.lock pyproject.toml /app/

# Install dependencies from pyproject.toml
RUN poetry install

# Copy all source code into container
COPY . /app/

CMD ["poetry", "run", "python", "src/main.py"]
