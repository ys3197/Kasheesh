from flask import Flask, render_template, request, json, Response
import sqlite3
import pandas as pd
app = Flask(__name__)


@app.route('/userid/<int:id>')
def show_records_userid(id):
    # Shows the information with given id.
    db = sqlite3.connect('Kasheesh.db')
    print("Opened database successfully")

    # get data from purchases and returns respectively
    sql_query_return = pd.read_sql_query ('''
                                select user_id, merchant_type_code,
                                amount_cents,datetime(datetime)
                                as datetime, transaction_type from returns
                                where user_id = {}
                               '''.format(id), db)

    sql_query_purchase = pd.read_sql_query ('''
                                select user_id, merchant_type_code,
                                amount_cents,datetime(datetime)
                                as datetime, transaction_type from purchases
                                where user_id = {}
                               '''.format(id), db)

    # convert into pandas dataframe and combine two tables
    df_return = pd.DataFrame(sql_query_return, columns = ['user_id', 'merchant_type_code', 'amount_cents', 'datetime', 'transaction_type'])
    df_purchase = pd.DataFrame(sql_query_purchase, columns = ['user_id', 'merchant_type_code', 'amount_cents', 'datetime', 'transaction_type'])
    df = pd.concat([df_return, df_return], ignore_index = True)

    # ouput and deal with errors
    if len(df) > 0:
        print("Data grabbed successfully")
        return Response(df.to_json(orient="records"), status=200, mimetype='application/json')
    else:
        return "User not found", 404

@app.route('/merchanttypecode/<int:merchant_type>')
def show_records_merchant(merchant_type):
    # Shows the post with given id.
    db = sqlite3.connect('Kasheesh.db')
    print("Opened database successfully")

    sql_query = pd.read_sql_query ('''
                               SELECT merchant_type_code, sum(amount_cents)/100 as net_amount_in_dollars,
                               date(datetime) as date FROM purchases where merchant_type_code = {}
                               group by merchant_type_code, date(datetime)
                               '''.format(merchant_type), db)

    df = pd.DataFrame(sql_query, columns = ['merchant_type_code', 'net_amount_in_dollars', 'date'])

    # ouput and deal with errors
    if len(df) > 0:
        print("Data grabbed successfully")
        return Response(df.to_json(orient="records"), status=200, mimetype='application/json')

    else:
        return "Merchant not found", 404





