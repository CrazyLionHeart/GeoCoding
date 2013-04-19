GeoCoding
=========

Утилита для определения координат с помощью провайдеров гео-кодинга: Яндекс, Гугль, Майкрософт

Исходные данные:

 - Таблица с адресами:
<pre>
CREATE TABLE address
(
  id serial NOT NULL,
  zipcode character varying(6) NOT NULL, -- Почтовый индекс
  street character varying(255) NOT NULL, -- Название улицы
  dom character varying(20) NOT NULL, -- Номер дома
  gorod character varying(60) NOT NULL, -- Название города
  g_coord point, -- Google coord of address
  y_coord point, -- Yandex coords of address
  m_coord point, -- Microsoft coord
)
</pre>

Задача: заполнить таблицу максимально быстро учитывая что:
- Google дает только 1000 адресов в сутки, 
- Microsoft дает 10000 запросов (правда, новый API-key можно получить из web-карт),
- Яндексу вообще по барабану - единственные кто честно отвечал на все запросы,
