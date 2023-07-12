from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import URL
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import select, insert, update, delete
from typing import Optional
from typing import List
import config2 as config
import requests

url_object = URL.create("postgresql+psycopg2", username=config.dbuser, password=config.dbpassword, host="localhost", database=config.dbName, port=5432)

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
    link: Mapped[str] 
    key_skills: Mapped[str] #= mapped_column(primary_key=True)

    # Текстовое представление объекта
    def __repr__(self):
        return(f"{self.company_name}, {self.position}, {self.job_description}, {self.key_skills}")
    
# Создаем таблицу
Base.metadata.create_all(engine)

i = 0
j = 0
lines = []
while True:
    url = f"https://api.hh.ru/vacancies?text={config.searchText}&page={i}&per_page=20"
    print(url)
    result = requests.get(url)
    i+=1

    print(result.status_code)
    if result.status_code != 200:
        continue

    vac = result.json().get('items')
    if len(vac)==0:
        break

    for k, v in enumerate(vac):
        print(k + 1, v['name'], v['url'], v['alternate_url'])
        result = requests.get(v['url'])
        print(result.status_code)
        if result.status_code != 200:
            continue
        vacancy = result.json()
        j+=1
        lines.append(vacancies(id=j, company_name=vacancy.get('employer', {}).get('name', ''), position=vacancy.get('name',''), job_description=vacancy.get('description',''), link=v['url'], key_skills=','.join([x['name'] for x in vacancy.get('key_skills', {'name': ''})])))

print('!!!!!!!!!!! end of loop')
print(len(lines))

with Session(engine) as session:
    for line in lines:
        session.add(line)
    session.commit()
    
print('end')