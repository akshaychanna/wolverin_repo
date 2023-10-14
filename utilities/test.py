
import logging
from logger_module import CustomSMTPHandler
# Configure logger
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.ERROR)

mail_handler = CustomSMTPHandler(
    mailhost=('smtp.gmail.com', 587),
    fromaddr='santhosh@advarisk.com',
    toaddrs=['solomon.santhosh@gmail.com'],
    subject='Error Occurred',
    credentials=('santhosh@advarisk.com', 'Santy@023@AdvaRisk')
)

logger.addHandler(mail_handler)

# Usage:
logger.error('An error occurred!')
