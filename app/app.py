from flask import Flask,request,jsonify
from flask_api import status
from flask import make_response
import config
import os
import traceback
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.utils import secure_filename
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import logging
import psycopg2
import random
import csv
from celery import Celery



app = Flask(__name__)

log = logging.getLogger('pydrop')


# app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://anvgag:password@localhost/products'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 
app.config['CELERY_BROKER_URL'] = 'amqp://localhost//'

def make_celery(app):
    celery = Celery(app.import_name,
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery



celery =  make_celery(app)
db = SQLAlchemy(app)


class Product(db.Model):
    __tablename__ = 'products'

    sku = db.Column(db.String(), primary_key=True)
    name = db.Column(db.String())
    description = db.Column(db.String())
    status = db.Column(db.String())

    def __init__(self, name, sku, description,status):
        self.sku = sku
        self.name = name
        self.description = description
        self.status = status
    
    def __repr__(self):
        return '<id {}>'.format(self.id)
    
    def serialize(self):
        return {
            'sku':self.sku,
            'name': self.name,
            'description': self.description,
            'status': self.status
        }
    
# def f1(x):
#     if '\n' in x:
#         return x.replace('\n','')
#     else:
#         return x

    
# def csv_to_db(path):
#     try:
#         state = ['active','disabled']
#         df = pd.read_csv(path)
#         print(df.shape)
#         order = ['sku','name','description','status']
#         df = df.drop_duplicates(subset=['sku'],keep='last')
#         df['description'] = df['description'].apply(f1)
#         df['status'] = [random.choice(state) for i in range(df.shape[0])]
#         df = df[order]   
#         df.to_csv('/home/anvgag/fulfil_assignment/data/cleaned_csv.csv',index=False,header=False)
#         del df
#         conn = psycopg2.connect("host='localhost' dbname='products' user='anvgag' password='password'")
#         cur = conn.cursor()
#         f = open('/home/anvgag/fulfil_assignment/data/cleaned_csv.csv', 'r')
#         cur.copy_from(f,'products',sep=',')
#         f.close()
#         conn.commit()
#         conn.close()
#     except Exception as e:
#         print(e)    
        
@celery.task(name="celery.insert_records")
def create_record(save_path):
    state = ['active','disabled']
    header_dict = {}  #Used to find which column number contains the sku,name,description
    with open(save_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        temp = next(csv_reader)
        header_dict['name'] = temp.index('name')
        header_dict['sku'] = temp.index('sku')
        header_dict['description'] = temp.index('description')
        for row in csv_reader:
            dup_check =Product.query.filter_by(sku=row[header_dict['sku']])
            if dup_check.scalar() is None:
                product=Product(
                name = row[header_dict['name']],
                sku = row[header_dict['sku']],
                description = row[header_dict['description']],
                status = random.choice(state)
                )
                db.session.add(product)
                db.session.commit()
            else:
                dup_check.name = row[header_dict['name']]
                dup_check.description = row[header_dict['description']]
                dup_check.status = random.choice(state)
                db.session.commit()      
            
        
@app.route('/upload_bulk',methods = ['POST'])    
def upload():
    '''
    Uploading
    '''
    file = request.files['file']
    current_chunk = int(request.form['dzchunkindex'])
    save_path = os.path.join('/home/anvgag/fulfil_assignment/data', secure_filename(file.filename))

    # If the file already exists it's ok if we are appending to it,
    # but not if it's new file that would overwrite the existing one
    if os.path.exists(save_path) and current_chunk == 0:
        # 400 and 500s will tell dropzone that an error occurred and show an error
        return make_response(('File already exists', 400))
    try:
        with open(save_path, 'ab') as f:
            f.seek(int(request.form['dzchunkbyteoffset']))
            f.write(file.stream.read())
    except OSError:
        return make_response(("Not sure why,"
                              " but we couldn't write the file to disk", 500))

    total_chunks = int(request.form['dztotalchunkcount'])

    if current_chunk + 1 == total_chunks:
        if os.path.getsize(save_path) != int(request.form['dztotalfilesize']):
            log.error(f"File {file.filename} was completed, "
                      f"but has a size mismatch."
                      f"Was {os.path.getsize(save_path)} but we"
                      f" expected {request.form['dztotalfilesize']} ")
            return make_response(('Size mismatch', 500))
        else:
            log.info(f'File {file.filename} has been uploaded successfully')
            #csv_to_db(save_path)
            create_record.delay(save_path)
    else:
        log.debug(f'Chunk {current_chunk + 1} of {total_chunks} '
                  f'for file {file.filename} complete')
    return make_response(("Chunk upload successful", 200))

@app.route('/insert',methods = ['POST'])
def insert():
    '''
    Inserting records into db
    '''
    state = ['active','disabled']
    if request.method == 'POST':
        name=request.form.get('name')
        sku=request.form.get('sku')
        description=request.form.get('description')
        status = random.choice(state)
        try:
            dup_check =Product.query.filter_by(sku=sku)
            if dup_check.scalar() is None:
                product=Product(
                name = name,
                sku = sku,
                description = description,
                status = random.choice(state)
                )
                db.session.add(product)
                db.session.commit()
            else:
                dup_check.name = name
                dup_check.description = description
                dup_check.status = random.choice(state)
                db.session.commit()      
            return "Product added"
        except Exception as e:
            return(str(e))
    return make_response(jsonify(message),status.HTTP_200_OK)

@app.route('/get',methods = ['GET'])
def get():
    '''Fetching records from db'''
    offset = request.args['offset']
    limit = request.args['limit']  
    search = request.args['search']
    if search is None:
        try:            
            temp = Product.query.offset(offset).limit(limit)
            rows = Product.query.count()
            products = [e.serialize() for e in temp]
            return jsonify({"total": rows,"totalNotFiltered": rows,"rows":products})
        except Exception as e:
            print(str(e))
            return(str(e))
    else:
        try:            
            temp = Product.query.filter(Product.status.like('%' + search + '%')).offset(offset).limit(limit)
            rows = Product.query.filter(Product.status.like('%' + search + '%')).count()
            products = [e.serialize() for e in temp]
            return jsonify({"total": rows,"totalNotFiltered": rows,"rows":products})
        except Exception as e:
            print(str(e))
            return(str(e))

@app.route('/update',methods = ['POST'])
def update():
    '''
    updating records in db 
    '''
    return make_response(jsonify({"Not implemented"}),status.HTTP_200_OK)
    
@app.route('/drop_table',methods = ['POST'])
def delete():
    '''
    deleting records in db
    '''
    try:
        conn = psycopg2.connect("dbname='products' user='anvgag' host='localhost' password='password'")
        cur = conn.cursor()
        cur.execute('DROP TABLE "products";')  
        conn.commit()
        conn.close()
        return make_response(jsonify({"message:Table dropped"}),status.HTTP_200_OK)
    except :
        return make_response(jsonify({"message:Table dropped"}),status.HTTP_200_OK)

@app.route('/create',methods = ['POST'])
def create_table():
    try:
        db.create_all()
        return make_response(jsonify({"message":"Table created"}),status.HTTP_200_OK)
    except:
        return make_response(jsonify({"message":"Table not created"}),status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@app.errorhandler(404)
def not_found(error):
    traceback.print_exc()
    obj = {"status": "KO", "host": request.remote_addr}
    obj['error'] = 'Not found'
    return make_response(jsonify(obj), status.HTTP_404_NOT_FOUND)


@app.errorhandler(400)
def bad_request(error):
    traceback.print_exc()
    obj = {"status": "KO", "host": request.remote_addr}
    obj['error'] = 'Bad Request, Invalid JSON input'
    return make_response(jsonify(obj), status.HTTP_400_BAD_REQUEST)


@app.errorhandler(Exception)
def internal_server_error(error):
    traceback.print_exc()
    obj = {"status": "KO", "host": request.remote_addr}
    obj['error'] = 'Application Error'
    return make_response(jsonify(obj), status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    
    
if __name__ == '__main__':
    app.run(debug=True)