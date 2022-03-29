from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    #Your code here
    
    committed_order= commit_new_order(order)
    existing_order= match_order(committed_order)

    if(existing_order is not None):
        new_order_left = commit_derived_order_obj(committed_order, existing_order)
        if(new_order_left is not None):
            process_order(new_order_left)

    else:
        return


def commit_new_order(new_order):
    committed_order = Order(sender_pk = new_order['sender_pk'], receiver_pk = new_order['receiver_pk'], buy_currency = new_order['buy_currency'],
                                sell_currency = new_order['sell_currency'], buy_amount = new_order['buy_amount'], sell_amount = new_order['sell_amount'])
    session.add(committed_order)
    session.commit()

    return committed_order


def match_order(new_order):
    existing_order = session.query(Order).filter(Order.filled == None, Order.sell_currency == new_order.buy_currency, Order.buy_currency == new_order.sell_currency,
                                                     ((Order.sell_amount / Order.buy_amount) >= (new_order.buy_amount / new_order.sell_amount)),
                                                     Order.sell_amount != Order.buy_amount, new_order.buy_amount != new_order.sell_amount)
    return existing_order.first()


def commit_derived_order_obj(committed_order, existing_order):

    committed_order.filled = datetime.now()
    existing_order.filled = datetime.now()

    committed_order.counterparty_id = existing_order.id
    existing_order.counterparty_id = committed_order.id

    if committed_order.buy_amount > existing_order.sell_amount:

        remaining = committed_order.buy_amount - existing_order.sell_amount
        ex_rate = committed_order.buy_amount / committed_order.sell_amount

        derived_order = Order(creator_id = committed_order.id, sender_pk = committed_order.sender_pk, receiver_pk = committed_order.receiver_pk, buy_currency = committed_order.buy_currency,
                                  sell_currency = committed_order.sell_currency, buy_amount = remaining, sell_amount = remaining / ex_rate)
        session.add(derived_order)
        session.commit()

    elif committed_order.buy_amount < existing_order.sell_amount:

        remaining = existing_order.sell_amount - committed_order.buy_amount
        ex_rate = existing_order.sell_amount / existing_order.buy_amount

        derived_order = Order(creator_id = existing_order.id, sender_pk = existing_order.sender_pk, receiver_pk = existing_order.receiver_pk, buy_currency = existing_order.buy_currency,
                                  sell_currency = existing_order.sell_currency, buy_amount = remaining / ex_rate, sell_amount = remaining)
        session.add(derived_order)
        session.commit()

    else:
        session.commit()
