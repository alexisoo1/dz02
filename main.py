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
import config
import logging 
from datetime import datetime
import requests
from bs4 import BeautifulSoup

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

urlBase = f'https://hh.ru/search/vacancy?text={config.searchText}&area=1'

i = 0
j = 0
lines = []
while True:
    if i >=1:
        url = f'{urlBase}&page={i}'
    else:
        url = urlBase

    result = requests.get(url, headers=config.user_agent)
    i+=1

    print(result.status_code)

    with open(f'page.txt{i}', 'w', encoding='UTF-8') as f:
        f.write(result.content.decode())
    # Создаем объект soup на основе загруженной Web-страницы
    soup = BeautifulSoup(result.content.decode(), 'lxml')
    
    names = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
    ln = len(names)
    if ln == 0:
        break
    print(ln)
    if i > 10:
        break
    for name in names:
        j+=1
        lines.append(vacancies(id=j, company_name='', position=name.text, job_description='', link=name.get('href'), key_skills=''))

print('!!!!!!!!!!! end of loop')
print(len(lines))

with Session(engine) as session:
    for line in lines:
        session.add(line)
    session.commit()

stmt = select(vacancies)
print(stmt)
with engine.connect() as conn:
    results = conn.execute(stmt)
    for row in results:

        result = requests.get(row.link, headers=config.user_agent)

        print(result.status_code)

        # Создаем объект soup на основе загруженной Web-страницы
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        
        pos = soup.find('h1')
        if pos:
            print(pos.text)
            print(pos.attrs)
        
        com = soup.find('a', attrs={'data-qa': 'vacancy-company-name'})
        if com:
            print(com.text)

        com = soup.find('div', {"class": "vacancy-company-details"})
        if com:
            print(com.text)

        des = soup.find('div', {"class": "g-user-content"}) 
        if des:
            print(des.text)

        ski = soup.find('div', {"class": "bloko-tag-list"}) 
        skills = []
        if ski:
            for s in ski:
                print(s.text)
                skills.append(s.text)
            print(ski.text)
        session = Session(engine)
        vac = session.execute(select(vacancies).where(vacancies.id == row.id)).scalar_one()
        vac.company_name = com.text
        vac.position = pos.text
        vac.job_description = des.text
        vac.key_skills = ','.join(skills)
        session.commit()
        session.close()
    
print('end')