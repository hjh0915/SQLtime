import db
import utils

if __name__ == '__main__':
    
    x = utils.get_data('initdata/data.txt')
    d = db.DB()
    d.write_data(x)