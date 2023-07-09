from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import URL
from sqlalchemy import select
from typing import Optional
from typing import List
import config
import logging 
from datetime import datetime
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(config.loggerLevel)
lfm = logging.Formatter(config.formatter)
lsh = logging.StreamHandler()
lsh.setLevel(config.streamLogHandlerLevel)
lsh.setFormatter(lfm)
lfh = logging.FileHandler(filename='log.log', mode='w')
lfh.setFormatter(lfm)
lfh.setLevel(config.fileLogHandlerLevel)
logger.addHandler(lsh)
logger.addHandler(lfh)

#url = "postgresql+psycopg2://test:12345@localhost:5433/postgres"
url_object = URL.create("postgresql+psycopg2", username="test", password="12345", host="localhost", database="vac", port=5432)

print(url_object)

engine = create_engine(url_object, echo=True)

# Базовый класс для создание классов, которые будут сохраняться в базе данных
class Base(DeclarativeBase):
    pass

# Создаем класс и описываем отображение атрибутов в столбцы таблицы базы данных
class vacancies(Base):
    # Название таблицы в базе данных
    __tablename__ = 'vacancies'

    # Атрибуты  
    id: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[str] 
    position: Mapped[str]
    job_description: Mapped[str] 
    key_skills: Mapped[str] #= mapped_column(primary_key=True)

    # Текстовое представление объекта
    def __repr__(self):
        return(f"{self.company_name}, {self.position}, {self.job_description}, {self.key_skills}")
    
# Создаем таблицу
Base.metadata.create_all(engine)

# new_vac = vacancies(id=1, company_name='Анна', position='ppp', job_description='ddd', key_skills='')
# with Session(engine) as session:
#     session.add(new_vac)
#     session.commit()

result = requests.get(config.url, headers=config.user_agent)

print(result.status_code)

# Создаем объект soup на основе загруженной Web-страницы
soup = BeautifulSoup(result.content.decode(), 'lxml')

print(soup.prettify())

name = soup.find('h1')

name = soup.find('a', attrs={'data-qa': 'vacancy-company-name'})

print('end')