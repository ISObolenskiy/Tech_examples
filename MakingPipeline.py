#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import datetime
import sqlite3
import great_expectations as gx

#Загружаем csv, сразу парсим Даты как тип "Дата":
df_ads = pd.read_csv('ads.csv', parse_dates=['created_at'])
df_leads = pd.read_csv('leads.csv', parse_dates=['lead_created_at'])
df_purchases = pd.read_csv('purchases.csv', parse_dates=['purchase_created_at'])

#Забираем наборы для проверки на Data Quality
ads_ge = gx.from_pandas(df_ads)
leads_ge = gx.from_pandas(df_leads)
purchases_ge = gx.from_pandas(df_purchases)

print("Проверка на полноту Ads, результат:", ads_ge.expect_column_values_to_not_be_null(
    column = {
        'created_at',
        'd_ad_account_id',
        'd_utm_source',
        'd_utm_medium',
        'd_utm_campaign',
        'd_utm_content'
    }
).success)

print("Проверка на полноту Leads, результат:", leads_ge.expect_column_values_to_not_be_null(
    column = {
        'lead_created_at',
        'lead_id'
    }
).success)

print("Проверка на уникальность ключевых полей в Leads, результат:", leads_ge.expect_column_values_to_be_unique(
   column = 'lead_id',
   meta = {
     "dimension": 'Uniqueness'
   }
).success)

print("Проверка на полноту Purchases, результат:", purchases_ge.expect_column_values_to_not_be_null(
    column = {
        'purchase_created_at',
        'purchase_id',
        'client_id',
        'm_purchase_amount'
    }
).success)

print("Проверка на уникальность ключевых полей в Purchases, результат:", purchases_ge.expect_column_values_to_be_unique(
   column = 'purchase_id',
   meta = {
     "dimension": 'Uniqueness'
   }
).success)

#Удаляем полные дубли, если есть:
df_ads=df_ads.drop_duplicates()
df_leads=df_leads.drop_duplicates()
df_purchases=df_purchases.drop_duplicates()

#Переводим поля в датафреймах в str для соответствия типов данных между датафреймами:
df_ads[['d_utm_campaign','d_utm_content','d_utm_term']]=df_ads[['d_utm_campaign','d_utm_content','d_utm_term']].astype(str)
df_leads[['d_lead_utm_campaign','d_lead_utm_content','d_lead_utm_term']]=df_leads[['d_lead_utm_campaign','d_lead_utm_content','d_lead_utm_term']].astype(str)

#Создадим датафрейм Лид-продажи

#Основная идея - Lead_ID для Purchases по условию - не более 15 дней
#merge в Pandas с этой задачей справляется слабо, а SQL отлично. поэтому сделаем объединение в SQLLite в памяти системы
conn = sqlite3.connect(':memory:')

#Сами таблички

df_ads.to_sql('ads', conn, index=False)
df_leads.to_sql('leads', conn, index=False)
df_purchases.to_sql('purchase', conn, index=False)

#Важно! Обязательно при left Join учитываем возможность того, 
#что по CLient_ID может быть привязана покупка, совершенная раньше, чем был лид.
#Такие случаи исключаем
#Сам запрос:
qry = '''
    select purchase_id,max(lead_id) as lead_id,purchase_created_at,m_purchase_amount 
    from (
    select  
        leads.*,
        purchase_created_at,purchase_id,m_purchase_amount,
        JulianDay(purchase_created_at)-JulianDay(lead_created_at) as "days_before_purchase"
    from
        leads
        left join purchase on (
                                JulianDay(purchase_created_at)-JulianDay(lead_created_at)
                                )<=15
                                and (
                                        JulianDay(purchase_created_at)-JulianDay(lead_created_at)
                                        )>=0 
                                and leads.client_id=purchase.client_id
    where purchase_id is not null 
        ) a
    group by purchase_id,purchase_created_at,purchase_id,m_purchase_amount
    '''
df = pd.read_sql_query(qry, conn)

#Объединяем результат с Ads и Leads:

merged_df=pd.merge(df_ads, df_leads, 
         left_on=['created_at','d_utm_source','d_utm_medium','d_utm_campaign','d_utm_content','d_utm_term'],
         right_on=['lead_created_at','d_lead_utm_source','d_lead_utm_medium','d_lead_utm_campaign','d_lead_utm_content','d_lead_utm_term'],
         how='left').merge(df, on='lead_id', how='left')

#Получаем датафрейм для пайплайна:
total_df = merged_df[['created_at','d_ad_account_id','d_utm_source','d_utm_medium','d_utm_campaign','d_utm_content','d_utm_term','m_clicks','m_cost','lead_id','client_id','purchase_created_at','purchase_id','m_purchase_amount']]
total_df['d_utm_term'].replace('nan', np.NaN, inplace=True)

#Пайплайн:
pipeline=pd.pivot_table(total_df, index=['created_at', 'd_utm_source','d_utm_medium','d_utm_campaign'],
                          values=['m_clicks', 'm_cost','lead_id','m_purchase_amount','purchase_id'],
                                aggfunc={
                                    'm_clicks':sum, 
                                    'm_cost':sum, 
                                    'lead_id':"count",
                                    'm_purchase_amount':sum, 
                                    'purchase_id':"count"
                                }
                       ).reset_index()
#Готовим пайплайн для GS
pipeline.rename(columns = {
        'created_at':'ДАТА',
        'lead_id':'Количество лидов',
        'm_clicks':'Количество кликов',
        'm_cost':'Расходы на рекламу',
        'm_purchase_amount':'Выручка от продаж',
        'purchase_id':'Количество покупок'
    }, inplace = True)

#Добавляем CPL, ROAS, где не было лидов и продаж ошибку деления на 0 (inf) обнуляем
pipeline['CPL']=pipeline['Расходы на рекламу']/pipeline['Количество лидов']
pipeline['CPL'].replace([np.inf, - np.inf], 0, inplace = True)
pipeline['ROAS']=pipeline['Выручка от продаж']/pipeline['Расходы на рекламу']
pipeline['ROAS'].replace([np.inf, - np.inf], 0, inplace = True)

pipeline_ge = gx.from_pandas(pipeline)

print("Проверка на полноту pipeline, результат:", pipeline_ge.expect_column_values_to_not_be_null(
    column = {
        'ДАТА',
        'd_utm_source',
        'd_utm_medium',
        'd_utm_campaign',
        'Количество лидов',
        'Количество кликов',
        'Расходы на рекламу',
        'Выручка от продаж',
        'Количество покупок'
    }
).success)

print("Проверка на размерность метрики ""Расходы на рекламу"" в pipeline, результат:", 
      pipeline_ge.expect_column_values_to_be_between(
          column = 'Расходы на рекламу',
          min_value = 0,
          meta = {
              "dimension": 'Consistency'
              }
          ).success)

print("Проверка на размерность метрики ""Выручка от продаж"" в pipeline, результат:", 
      pipeline_ge.expect_column_values_to_be_between(
          column = 'Выручка от продаж',
          min_value = 0,
          meta = {
              "dimension": 'Consistency'
              }
          ).success)

print('Uploading data')

pipeline.to_excel('Pipeline.xlsx', index=False)

print('Data Uploaded')

