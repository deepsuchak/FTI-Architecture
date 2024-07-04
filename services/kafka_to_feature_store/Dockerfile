FROM python:3.10.3-slim-buster


## we need this to install gcc which is a C compiler and python-dev 
## these two are to configure python dependencies that depend on C libraries
## Hopswroks needs tofi which is dependent on C libraries
## So when I poetry add hopsworks, i also need to add tofi dependecy (which is based on C)
## so we need a C compiler which is done via the below command
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev

# stream output to console
ENV PYTHONUNBUFFERED=1

# install poetry inside the container
RUN pip install poetry==1.8.2

WORKDIR /app

# add /app/src to PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app/

# install Python dependencies from the pyproject.toml file
RUN poetry install

# copy all the source code into the container
COPY src/*.py /app/src/

CMD ["poetry", "run", "python", "src/main.py"]