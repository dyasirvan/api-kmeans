from app import app
from flaskext.mysql import MySQL
mysql = MySQL()
 
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'bfc59d60bc4718'
app.config['MYSQL_DATABASE_PASSWORD'] = 'a58fcc55'
app.config['MYSQL_DATABASE_DB'] = 'heroku_f4425b09c2cb37b'
app.config['MYSQL_DATABASE_HOST'] = 'us-cdbr-east-02.cleardb.com'
mysql.init_app(app)