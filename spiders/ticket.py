import json
import re
from dataclasses import dataclass


@dataclass
class Ticket:

    def __init__(self, ticket: dict | str):
        if isinstance(ticket, str):
            ticket = json.loads(ticket)
        self.sector = ticket.pop('sector', None) or ''
        self.sector = re.sub(r'\s+', ' ', self.sector).strip()
        self.row = self.seat = None
        self.stand = ticket.pop('stand', False)
        if not self.stand:
            self.row = re.sub(
                r'(?i:ложа|ряд)', '',
                str(ticket.pop('row', None) or '')).strip()
            self.seat = ticket.pop('seat', None) or ''
        self.price = ticket.pop('price', None)
        self.success = ticket.pop('success', None)
        self.client = ticket.pop('client', None)
        for key, value in ticket.items():
            setattr(self, key, value)
        super().__init__()

    def __getitem__(self, key):
        return getattr(self, key, None)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key):
        return getattr(self, key, None)

    def get_dict(self):
        return {k: v for k, v in self.__dict__.items() if v}

    def get_sid(self):
        return f'{self.sector}_{self.row}_{self.seat}'

    def __repr__(self):
        return f'Ticket({str(self)})'

    def __json__(self):
        return json.dumps(self.get_dict())

    def __str__(self):
        if self.stand:
            return (f'Стоячий сектор: {self.sector} '
                    f'цена: {self.price} (цена за один билет)')
        return (f'Сектор: {self.sector}, ряд: {self.row}, '
                f'место: {self.seat}, цена: {self.price}')
