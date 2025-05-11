from .constants import RecordCode
from .models import Record


def _build_account_identifier_record(rows):
    """
    Flatten a 03 header and its 88 continuations into one BAI “account-header”
    `Record`.  All 03/88 bodies are split on “,” (retaining empty tokens),
    merged, and excess blanks are trimmed so each summary-code group is exactly
    four fields.  The resulting `Record` holds:
        fields = [account_id, currency, <code, amount, item_cnt, funds_type>*]
        rows   = the original (03 + 88 …) tuples
    """

    def _tokens(line: str) -> list[str]:
        """Split, trim, and keep the trailing comma (=> empty token)."""
        return [t for t in line.rstrip("/").split(",")]

    _, body03 = rows[0]
    account_identifier, currency, *field_buffer = _tokens(body03)

    for _, body88 in rows[1:]:
        field_buffer.extend(_tokens(body88))

    # Clean up excess blanks - each record is consist of a group of 4 fields:
    # type code, amount, item count, funds type. Only last two fields can be blank.
    cleaned_fields: list[str] = []
    for token, field_group in groupby(field_buffer):
        token_count_in_group = len(list(field_group))
        if token != "":
            cleaned_fields.extend([token] * token_count_in_group)
        else:
            cleaned_fields.extend([""] * min(token_count_in_group, 2))

    fields = [account_identifier, currency] + cleaned_fields
    return Record(rows[0][0], fields=fields, rows=rows)


def _build_generic_record(rows):
    fields_str = ''
    for row in rows:
        field_str = row[1]

        if field_str:
            if field_str[-1] == '/':
                fields_str += field_str[:-1] + ','
            else:
                fields_str += field_str + ' '

    fields = fields_str[:-1].split(',')
    return Record(code=rows[0][0], fields=fields, rows=rows)


RecordBuilderFactory = {
    RecordCode.file_header: _build_generic_record,
    RecordCode.group_header: _build_generic_record,
    RecordCode.account_identifier: _build_account_identifier_record,
    RecordCode.transaction_detail: _build_generic_record,
    RecordCode.account_trailer: _build_generic_record,
    RecordCode.group_trailer: _build_generic_record,
    RecordCode.file_trailer: _build_generic_record,
}


def _build_record(rows):
    record_code = rows[0][0]
    return RecordBuilderFactory[record_code](rows)


def record_generator(lines):
    rows = iter(
        [(RecordCode(line[:2]), line[3:]) for line in lines]
    )

    records = [next(rows)]
    while True:
        try:
            row = next(rows)
        except StopIteration:
            break

        if row[0] != RecordCode.continuation:
            yield _build_record(records)
            records = [row]
        else:
            records.append(row)

    yield _build_record(records)


class IteratorHelper:
    def __init__(self, lines):
        self._generator = record_generator(lines)
        self.current_record = None
        self.advance()

    def advance(self):
        self.current_record = next(self._generator)
