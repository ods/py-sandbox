from dataclasses import dataclass
import sqlalchemy as sa
from sqlalchemy.engine.interfaces import ExecutionContext
from sqlalchemy.orm.context import ORMSelectCompileState, QueryContext
import sqlalchemy.orm.loading
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    Session,
    class_mapper,
    attributes,
    composite,
)


class Base(DeclarativeBase):
    pass


@dataclass
class Point:
    x: int
    y: int


class User(Base):
    __tablename__ = "users"

    def __new__(cls, *args, **kwargs):
        print(f"new: {args=}, {kwargs=}")
        if not kwargs:
            # 1️⃣ Where the instance is created from?
            # breakpoint()
            pass
        return super().__new__(cls)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column("name_in_db", sa.String(30))
    point: Mapped[Point] = composite(
        mapped_column("x", sa.Integer),
        mapped_column("y", sa.Integer),
    )

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, point={self.point!r})"


def instance_processor(*args, **kwargs):
    print(f"_instance_processor: {args=}, {kwargs=}")
    # 2️⃣ Where processor is created from?
    # breakpoint()
    orig_instance = orig_instance_processor(*args, **kwargs)

    def instance(*args, **kwargs):
        print(f"_instance: {args=}, {kwargs=}")
        # 3️⃣ Where instance is created from?
        # breakpoint()
        return orig_instance(*args, **kwargs)

    return instance


orig_instance_processor = sqlalchemy.orm.loading._instance_processor
sqlalchemy.orm.loading._instance_processor = instance_processor


def query_context_init(*args, **kwargs):
    print(f"QueryContext.__init__: {args=}, {kwargs=}")
    return orig_query_context_init(*args, **kwargs)

orig_query_context_init = QueryContext.__init__
QueryContext.__init__ = query_context_init


# ---

engine = sa.create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)

with Session(engine) as session, session.begin():
    bob = User(name="Bob", point=Point(12, 23))
    session.add(bob)

with Session(engine) as session:
    user = session.query(User).first()
    print(user)


# The results:
# 1️⃣
#   model.py(38)<module>()
# -> user = session.query(User).first()
#   sqlalchemy/orm/query.py(2728)first()
# -> return self.limit(1)._iter().first()  # type: ignore
#   sqlalchemy/engine/result.py(1786)first()
# -> return self._only_one_row(
#   sqlalchemy/engine/result.py(749)_only_one_row()
# -> row: Optional[_InterimRowType[Any]] = onerow(hard_close=True)
#   sqlalchemy/engine/result.py(1673)_fetchone_impl()
# -> return self._real_result._fetchone_impl(hard_close=hard_close)
#   sqlalchemy/engine/result.py(2259)_fetchone_impl()
# -> row = next(self.iterator, _NO_ROW)
#   sqlalchemy/orm/loading.py(223)chunks()
# -> rows = [proc(row) for row in fetch]
#   sqlalchemy/orm/loading.py(1114)_instance()
# -> instance = mapper.class_manager.new_instance()
#   sqlalchemy/orm/instrumentation.py(507)new_instance()
# -> instance = self.class_.__new__(self.class_)
# > model.py(16)__new__()
# -> return super().__new__(cls)
#
# 2️⃣
#   model.py(53)<module>()
# -> user = session.query(User).first()
#   sqlalchemy/orm/query.py(2728)first()
# -> return self.limit(1)._iter().first()  # type: ignore
#   sqlalchemy/orm/query.py(2827)_iter()
# -> result: Union[ScalarResult[_T], Result[_T]] = self.session.execute(
#   sqlalchemy/orm/session.py(2351)execute()
# -> return self._execute_internal(
#   sqlalchemy/orm/session.py(2236)_execute_internal()
# -> result: Result[Any] = compile_state_cls.orm_execute_statement(
#   sqlalchemy/orm/context.py(296)orm_execute_statement()
# -> return cls.orm_setup_cursor_result(
#   sqlalchemy/orm/context.py(587)orm_setup_cursor_result()
# -> return loading.instances(result, querycontext)
#   sqlalchemy/orm/loading.py(114)instances()
# -> query_entity.row_processor(context, cursor)
#   sqlalchemy/orm/context.py(2710)row_processor()
# -> _instance = loading._instance_processor(
# > model.py(30)instance_processor()
# -> orig_instance = orig_instance_processor(*args, **kwargs)
#
# 3️⃣
#   model.py(54)<module>()
# -> user = session.query(User).first()
#   sqlalchemy/orm/query.py(2728)first()
# -> return self.limit(1)._iter().first()  # type: ignore
#   sqlalchemy/engine/result.py(1786)first()
# -> return self._only_one_row(
#   sqlalchemy/engine/result.py(749)_only_one_row()
# -> row: Optional[_InterimRowType[Any]] = onerow(hard_close=True)
#   sqlalchemy/engine/result.py(1673)_fetchone_impl()
# -> return self._real_result._fetchone_impl(hard_close=hard_close)
#   sqlalchemy/engine/result.py(2259)_fetchone_impl()
# -> row = next(self.iterator, _NO_ROW)
#   sqlalchemy/orm/loading.py(223)chunks()
# -> rows = [proc(row) for row in fetch]
# > model.py(36)instance()
# -> return orig_instance(*args, **kwargs)
#
# QueryContext.__init__:
# args=(
#     <sqlalchemy.orm.context.QueryContext object at 0x102b82ea0>,
# )
# kwargs={
#     'compile_state': <sqlalchemy.orm.context.ORMSelectCompileState object at 0x102b5e330>,
#     'statement': <sqlalchemy.sql.selectable.Select object at 0x102b246b0>,
#     'params': {},
#     'session': <sqlalchemy.orm.session.Session object at 0x102b671a0>,
#     'load_options': <class 'sqlalchemy.orm.context.QueryContext.default_load_options'>
# }
#
# instance_processor:
# args=(
#     <sqlalchemy.orm.context._MapperEntity object at 0x10724ea40>,
#     <Mapper at 0x105639970; User>,
#     <sqlalchemy.orm.context.QueryContext object at 0x1072d92a0>,
#     <sqlalchemy.engine.cursor.CursorResult object at 0x10724eb30>,
#     CachingEntityRegistry((<Mapper at 0x105639970; User>,)),
#     None
# )
# kwargs={
#     'only_load_props': None,
#     'refresh_state': None,
#     'polymorphic_discriminator': None
# }
#
# instance:
# args=(
#     (1, 'Bob'),
# )
# kwargs={}

# ---
# Create instance from row

mapper = class_mapper(User)

with engine.connect() as db:
    result = db.execute(sa.select(User).filter_by(id=1))
    row = result.fetchone()

# Based on sqlalchemy/orm/loading.py(1329)_populate_full
context: ExecutionContext = result.context
assert context.compiled is not None
compile_state: ORMSelectCompileState = context.compiled.compile_state
assert compile_state is not None
quick_populators = compile_state.attributes["memoized_setups", (mapper,)]

instance = mapper.class_manager.new_instance()
dict_ = attributes.instance_dict(instance)
for prop, col in quick_populators.items():
    col = quick_populators[prop]
    getter = result._getter(col, False)
    dict_[prop.key] = getter(row)

print(instance)

# ---

mapper = class_mapper(User)

with engine.connect() as db:
    row_result = db.execute(sa.select(User).filter_by(id=1))

    query_context = QueryContext(
        compile_state=result.context.compiled.compile_state,
        statement=result.context.invoked_statement,
        params={},  # XXX
        session=Session(),
        load_options=QueryContext.default_load_options,
        # XXX execution_options, bind_arguments?
    )

    orm_result = sqlalchemy.orm.loading.instances(row_result, query_context)
    instance = orm_result.scalar_one_or_none()
    print(instance)
