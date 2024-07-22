class Config:
    SECRET_KEY = 'your_secret_key'
    SQLALCHEMY_DATABASE_URI = 'mysql://amsdb:ghrn1004!!@my8002.gabiadb.com/amsdb'
    #변경을 자동으로 추적하고, 변경 사항을 데이터베이스에 반영하는 옵션
    SQLALCHEMY_TRACK_MODIFICATIONS = False
