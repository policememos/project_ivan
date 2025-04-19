import re
from typing import Optional, Union

from spiders.ticket import Ticket


class Condition:  # pylint: disable=R0902

    def __init__(self, cond: dict, max_tickets: int = None):
        self.sector = self.__prepare_sector(cond)
        self.row = cond.get('re_rows') or self.__regex_cond(cond.get('rows'))
        self.seat = cond.get('re_seats') or self.__regex_cond(cond.get('seats'))
        self.price_min = cond.get('price_min')
        self.price_max = cond.get('price_max')
        # self.units = self.__prepare_units(cond, max_tickets)
        # self.index = cond.get('index')
        self.count = cond.get('count')
        # self.priority = cond.get('priority') or 0
        # self.promocode = cond.get('promocode') or None
        # self.sort = self.__make_tuple(cond.get('sort')) or None
        # self.sort_index = cond.get('sort_index') or None

    def __str__(self):
        return str({k: v for k, v in self.__dict__.items() if v is not None})

    @staticmethod
    def __prepare_sector(cond: dict) -> Optional[str]:
        sector = fr'^(?i:{cond["sector"]})$' if cond.get('sector') else None
        return cond.get('re_sector') or sector

    @staticmethod
    def __regex_cond(text: Optional[str]) -> Optional[str]:
        """Convert '1,3,5-7' into '^(?i:1|3|5|6|7)$'."""
        if not text:
            return None
        parts = []
        for part in text.split(','):
            ranges = part.split('-')
            if len(ranges) == 1:
                parts.append(part)
            else:
                r_start = int(ranges[0])
                r_finish = int(ranges[-1]) + 1
                parts += [str(numb) for numb in range(r_start, r_finish)]
        return fr'^(?i:{"|".join(parts)})$'

    # @staticmethod
    # def __prepare_units(cond: dict, max_tickets: int) -> Optional[list]:
    #     if cond.get('pairs'):
    #         max_tickets = max_tickets or 4
    #         units = []
    #         for ind in range(2, max_tickets + 1):
    #             units.append([str(u) for u in range(1, ind + 1)])
    #     elif units := cond.get('units'):
    #         for ind, unit in enumerate(units):
    #             unit = unit if isinstance(unit, list) else [unit]
    #             units[ind] = [str(u) for u in unit]
    #     return sorted(units, key=len, reverse=True) if units else None

    # @staticmethod
    # def __make_tuple(sort):
    #     if isinstance(sort, list):
    #         return tuple([tuple(x) for x in sort])

    def check_sector(self, sector: str) -> bool:
        return not self.sector or re.search(self.sector, sector)

    def check_row(self, row: Union[str, int]) -> bool:
        return not self.row or re.search(self.row, str(row))

    def check_seat(self, seat: Union[str, int]) -> bool:
        return not self.seat or re.search(self.seat, str(seat))

    def check_price(self, price: Union[str, int, float]) -> bool:
        if self.price_min and self.price_min > int(price):
            return False
        if self.price_max and self.price_max < int(price):
            return False
        return True

    def check(self, ticket: Ticket) -> bool:  # noqa: C901 pylint: disable=R0911
        if self.count is not None and self.count <= 0:
            return False
        # if ticket.stand and self.units:
        #     return False
        if not self.check_sector(ticket.sector):
            return False
        if ticket.row is not None and not self.check_row(ticket.row):
            return False
        if ticket.seat is not None and not self.check_seat(ticket.seat):
            return False
        if ticket.price is not None and not self.check_price(ticket.price):
            return False
        # if not ticket.stand and not self.units and self.count:
        #     self.count -= 1
        # if self.promocode:
        #     ticket['promocode'] = self.promocode
        return True
