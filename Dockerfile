FROM python:3.6.13-stretch

ENV APP_ROOT=/app
WORKDIR $APP_ROOT

RUN pip install pipenv
ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN pipenv install

COPY . $APP_ROOT

CMD pipenv run python src/server.py

EXPOSE 5000
