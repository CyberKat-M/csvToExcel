from data_layer import initialize_database
import config

initialize_database(config.DATABASE_URI)
