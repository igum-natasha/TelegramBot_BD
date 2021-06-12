import logging
from functions import find_value_bd, clear_one_table, create_db, OutputDB, AddNewRecord, DeleteParam, DeleteID, \
    DeleteALL
from sqlalchemy import MetaData, text, create_engine, select
from sqlalchemy.exc import OperationalError, ArgumentError, ProgrammingError
from sqlalchemy.orm import sessionmaker
from setup import PROXY, TOKEN
from telegram import Bot, Update
from telegram.ext import CallbackContext, CommandHandler, Filters, MessageHandler, Updater, ConversationHandler
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)
array = []
login, passv, table, field, value, title = '', '', '', '', '', ''
res = None
engine = None
session = None
metadata = None


def analise(function):
    def inner(*args, **kwargs):
        update = args[0]
        if update and hasattr(update, 'message') and hasattr(update, 'effective_user'):
            array.append({
                "user": update.effective_user.first_name,
                "function": function.__name__,
                "message": update.message.text})
        return function(*args, **kwargs)

    return inner


def decorator_error(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SyntaxError:
            update = args[0]
            if update:
                update.message.reply_text(f'Error! Function:{func.__name__}')

    return inner


@decorator_error
@analise
def start(update: Update, context: CallbackContext):
    update.message.reply_text(f'Привет, {update.effective_user.first_name}!')


@decorator_error
@analise
def auth(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи свой логин!')
    return 'login'


@decorator_error
@analise
def auth_login(update: Update, context: CallbackContext):
    global login
    login = update.message['text']
    update.message.reply_text(f'Введи свой пароль!')
    return 'passwd'


@decorator_error
@analise
def auth_pass(update: Update, context: CallbackContext):
    global engine, session, metadata, passv
    passv = update.message['text']
    update.message.delete()
    try:
        print(login, passv)
        engine = create_engine('postgresql://{}:{}@localhost/Lab1'.format(login, passv), echo=True)
        engine.connect()
        metadata = MetaData()
        metadata.create_all(engine)
        metadata.reflect(bind=engine)
        Session = sessionmaker(bind=engine)  # bound session
        session = Session()
    except OperationalError:
        update.message.reply_text(f'Ошибка, некорректные данные!!!')
    else:
        update.message.reply_text(f'Готово!!!')
    return ConversationHandler.END


@decorator_error
@analise
def chat_help(update: Update, context: CallbackContext):
    update.message.reply_text('Введи команду /auth для авторизации. ')


@decorator_error
@analise
def find(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название таблицы для поиска!')
    return 'find'


@decorator_error
@analise
def delete_records(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название таблицы для удаления записей!')
    return 'delete'


@decorator_error
@analise
def delete_id(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название таблицы для удаления записей!')
    return 'delete'


@decorator_error
@analise
def clear(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название таблицы для очистки!')
    return 'table'


@decorator_error
@analise
def insert(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название таблицы для добавления записи!')
    return 'name'


@decorator_error
@analise
def create(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи название базы данных!')
    return 'name'


@decorator_error
@analise
def output_all(update: Update, context: CallbackContext):
    global session, metadata, res, title
    if session is None:
        update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
    result = OutputDB(metadata, session)
    for table in metadata.sorted_tables:
        update.message.reply_text("--------------- {} ---------------".format(table))
        title = list(engine.execute(select(['*']).select_from(table)).keys())
        res = result[table.name]
        output(update, context)
    update.message.reply_text("Готово!")


@decorator_error
@analise
def clear_many(update: Update, context: CallbackContext):
    update.message.reply_text(f'Введи названия таблицы для очистки через запятую!')
    return 'table'


@decorator_error
@analise
def create_entry(update: Update, context: CallbackContext):
    global login, passv, session
    if session is None:
        update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
        return ConversationHandler.END
    name = update.message['text']
    answer = create_db(name, login, passv)
    if not answer:
        update.message.reply_text("OK")
        return ConversationHandler.END
    update.message.reply_text("Такая база данных уже есть")
    return ConversationHandler.END


@decorator_error
@analise
def insert_value(update: Update, context: CallbackContext):
    global metadata, session, table
    try:
        if session is None:
            update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
            return ConversationHandler.END
        data = update.message['text']
        answer = AddNewRecord(table, data, session,  metadata)
        update.message.reply_text(f"OK")
        return ConversationHandler.END
    except Exception:
        update.message.reply_text(f"Ошибка, некорректные данные!")
        return ConversationHandler.END



def delete(table):
    global session, engine, metadata
    if session is None:
        return f'Ошибка, пользователь не авторизирован!'
    try:
        clear_one_table(metadata, engine, text(table))
        return f'Готово!!!'
    except ProgrammingError:
        return f'Ошибка, некорректные данные!!!'


@decorator_error
@analise
def clear_entry(update: Update, context: CallbackContext):
    table = update.message['text']
    answer = delete(table)
    update.message.reply_text(answer)
    return ConversationHandler.END


@decorator_error
@analise
def clear_mentry(update: Update, context: CallbackContext):
    global session, engine, metadata
    tables = update.message['text'].split(', ')
    print(tables)
    if session is None:
        update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
        return ConversationHandler.END
    answer = ''
    for table in tables:
        answer += f'{table} - {delete(table)}\n'
    update.message.reply_text(answer)
    return ConversationHandler.END


@decorator_error
@analise
def find_entry(update: Update, context: CallbackContext):
    global table
    table = update.message['text']
    update.message.reply_text(f'Введи название поля!')
    return 'field'


@decorator_error
@analise
def delete_entry(update: Update, context: CallbackContext):
    global table
    table = update.message['text']
    update.message.reply_text(f'Введи название не ключевого поля!')
    return 'field'


@decorator_error
@analise
def delete_id_entry(update: Update, context: CallbackContext):
    global table
    table = update.message['text']
    update.message.reply_text(f'Введи название ключевого поля!')
    return 'field'


@decorator_error
@analise
def insert_entry(update: Update, context: CallbackContext):
    global table
    table = update.message['text']
    title = list(engine.execute(select(['*']).select_from(text(table))).keys())
    update.message.reply_text(f'Введи значение для полей {title} через ";"!')
    return 'value'


@decorator_error
@analise
def find_field(update: Update, context: CallbackContext):
    global field
    field = update.message['text']
    update.message.reply_text(f'Введи значение поля {field}!')
    return 'value'


@decorator_error
@analise
def delete_field(update: Update, context: CallbackContext):
    global field
    field = update.message['text']
    update.message.reply_text(f'Введи значение поля {field}!')
    return 'value'


@decorator_error
@analise
def delete_id_field(update: Update, context: CallbackContext):
    global field
    field = update.message['text']
    update.message.reply_text(f'Введи значение поля {field}!')
    return 'value'


@decorator_error
@analise
def delete_db(update: Update, context: CallbackContext):
    res = DeleteALL(metadata, engine)
    update.message.reply_text(res)


@decorator_error
@analise
def delete_value(update: Update, context: CallbackContext):
    global table, field, value, title, metadata, session
    value = update.message['text']
    if not engine:
        update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
        return ConversationHandler.END
    value = '\'' + value + '\''
    res = DeleteParam(table, field, value, metadata, session)
    update.message.reply_text(res)
    return ConversationHandler.END


@decorator_error
@analise
def delete_id_value(update: Update, context: CallbackContext):
    global table, field, value, title, metadata, session
    value = update.message['text']
    if not engine:
        update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
        return ConversationHandler.END
    value = '\'' + value + '\''
    res = DeleteID(table, field, value, metadata, session)
    update.message.reply_text(res)
    return ConversationHandler.END


@decorator_error
@analise
def find_value(update: Update, context: CallbackContext):
    global res, table, field, value, title
    value = update.message['text']
    try:
        if not engine:
            update.message.reply_text(f'Ошибка, пользователь не авторизирован!')
            return ConversationHandler.END
        res = find_value_bd(engine, text(table), field, value)
        title = list(engine.execute(select(['*']).select_from(text(table))).keys())
        output(update, context)
    except ArgumentError:
        update.message.reply_text(f'Ошибка, некорректные данные!')
    return ConversationHandler.END


def output(update, context):
    global res, title
    answer = ''
    print(res, title)
    for e in res:
        for i in range(len(title)):
            answer += f'{title[i]} - {e[i]}\n'
        update.message.reply_text(answer)
        answer = ''
    return ConversationHandler.END

@analise
def echo(update: Update, context: CallbackContext):
    update.message.reply_text(update.message.text)


@analise
def error(update: Update, context: CallbackContext):
    logger.warning(f'Update {update} caused error {context.error}')


@decorator_error
@analise
def info(update: Update, context: CallbackContext):
    answer = 'Команды для работы с ботом:\n/start-Приветствие\n/help-Подсказка с чего начать\n' \
             '/auth-Авторизация\n/find-Найти запись по значению\n/clear_one-Очистить одну таблицу\n' \
             '/clear_many-Очистить все таблицы\n/create_db-Создать базу данных\n/output_all-Вывести все таблицы\n' \
             '/insert-Вставка новой строки\n/delete_record-Удаление записей по не ключевому полю\n' \
             '/delete_by_id-Удаление записей по ключевому полю\n/delete_db-Удаление базы данных'
    update.message.reply_text(answer)


def main():
    bot = Bot(
        token=TOKEN
    )
    updater = Updater(bot=bot, use_context=True)

    # on different commands - answer in Telegram
    updater.dispatcher.add_handler(CommandHandler('start', start))
    updater.dispatcher.add_handler(CommandHandler('help', chat_help))
    updater.dispatcher.add_handler(CommandHandler('info', info))
    updater.dispatcher.add_handler(ConversationHandler(entry_points=[MessageHandler(Filters.regex('/auth'), auth)],
                                                       states={'login':[MessageHandler(Filters.text, auth_login)],
                                                               'passwd': [MessageHandler(Filters.text, auth_pass)]},
                                                       fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(ConversationHandler(entry_points=[MessageHandler(Filters.regex('/find'), find)],
                                                       states={'find': [MessageHandler(Filters.text, find_entry)],
                                                               'field': [MessageHandler(Filters.text, find_field)],
                                                               'value': [MessageHandler(Filters.text, find_value)]},
                                                       fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/clear_one'), clear)],
                                                       states={'table': [MessageHandler(Filters.text, clear_entry)]},
                                                       fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/clear_many'), clear_many)],
                            states={'table': [MessageHandler(Filters.text, clear_mentry)]},
                            fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/create_db'), create)],
                            states={'name': [MessageHandler(Filters.text, create_entry)]},
                            fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/insert'), insert)],
                            states={'name': [MessageHandler(Filters.text, insert_entry)],
                                    'value': [MessageHandler(Filters.text, insert_value)]},
                            fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/delete_record'), delete_records)],
                                                       states={'delete': [MessageHandler(Filters.text, delete_entry)],
                                                               'field': [MessageHandler(Filters.text, delete_field)],
                                                               'value': [MessageHandler(Filters.text, delete_value)]},
                                                       fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(
        ConversationHandler(entry_points=[MessageHandler(Filters.regex('/delete_by_id'),
                                                                       delete_id)],
                                                    states={'delete': [MessageHandler(Filters.text, delete_id_entry)],
                                                            'field': [MessageHandler(Filters.text, delete_id_field)],
                                                            'value': [MessageHandler(Filters.text, delete_id_value)]},
                                                    fallbacks=[MessageHandler(Filters.text, error)]))
    updater.dispatcher.add_handler(CommandHandler('output_all', output_all))
    updater.dispatcher.add_handler(CommandHandler('delete_db', delete_db))

    # log all errors
    updater.dispatcher.add_error_handler(error)

    # Start the Bot
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    logger.info('Start Bot')
    main()
