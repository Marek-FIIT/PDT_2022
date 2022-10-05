import json
import csv
from typing import List, Optional
from pydantic import BaseModel, ValidationError, validator, root_validator
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import datetime

current_hashtag_id = 0
unique_hashtags = {}
unique_domains  = set()
unique_entities = set()

unique_conversations = set()
unique_authors = set()

authors_csv = open('./csvs/authors-01.csv', 'w', newline='', encoding='utf-8')
authors_writer = csv.writer(authors_csv, delimiter="|", escapechar="~")

log_csv = open('log.csv', 'w', newline='', encoding='utf-8')
log_writer = csv.writer(log_csv, delimiter=";")
log_writer.writerow(['block', 'current_time', 'total_duration', 'block_duration'])
start_time = time.time()
block_time = start_time


class Conversation(BaseModel):
    class PublicMetrics(BaseModel):
        retweet_count: Optional[int] = None
        reply_count:   Optional[int] = None
        like_count:    Optional[int] = None
        quote_count:   Optional[int] = None

    class ReferencedTweet(BaseModel):
        id: int
        type: str

    class Entities(BaseModel):
        class Annotation(BaseModel):
            normalized_text: str
            type: str
            probability: float

            @validator('normalized_text', 'type', always=True)
            def reformat_empty(cls, value):
                return value if value != "" else '""'
        
        class Url(BaseModel):
            expanded_url: str
            title: Optional[str] = None
            description: Optional[str] = None

            @validator('title', 'description', always=True)
            def default_none(cls, value):
                return value if value != "" else None
        
        class Hashtag(BaseModel):
            tag: str
            
            id: Optional[int]
            new: Optional[bool] = False

            @root_validator()
            def _set_fields(cls, values: dict) -> dict:
                try:
                    values["id"] = unique_hashtags[values["tag"]]
                except KeyError:
                    global current_hashtag_id
                    current_hashtag_id +=1
                    values["id"] = current_hashtag_id
                    unique_hashtags[values["tag"]] = current_hashtag_id
                    values["new"] = True
                
                return values
                    
        annotations: Optional[List[Annotation]] = []
        urls: Optional[List[Url]] = []
        hashtags: Optional[List[Hashtag]] = []

    class ContextAnnotation(BaseModel):
        class Domain(BaseModel):
            id: int
            name: str
            description: Optional[str] = None
            
            new: Optional[bool]

            @root_validator()
            def _set_fields(cls, values: dict) -> dict:
                if not values["id"] in unique_domains:
                    values["new"] = True
                    unique_domains.add(values["id"])

                if values["description"] == "":
                    values["description"] = None
                
                return values
        
        class Entity(BaseModel):
            id: int
            name: str
            description: Optional[str] = None

            new: Optional[bool] = False

            @root_validator()
            def _set_fields(cls, values: dict) -> dict:
                if not values["id"] in unique_entities:
                    values["new"] = True
                    unique_entities.add(values["id"])

                if values["description"] == "":
                    values["description"] = None
                
                return values


        domain: Domain
        entity: Entity

    id: int
    author_id: int
    text: str
    possibly_sensitive: bool
    lang: str
    source: str
    public_metrics: Optional[PublicMetrics] = None
    created_at: str
    referenced_tweets: Optional[List[ReferencedTweet]] = []
    entities: Optional[Entities] = None
    context_annotations: Optional[List[ContextAnnotation]] = []

    @validator('id', pre=True, always=True)
    def unique_id(cls, value, values):
        if value in unique_conversations:
            raise ValidationError(errors=None, model=None)

        unique_conversations.add(value)
        return value

    @validator('author_id', always=True)
    def check_authors(cls, value):
        if value in unique_authors:
            return value

        unique_authors.add(value)
        authors_writer.writerow([value, None, None, None, None, None, None, None])
        return value

    @validator('text', always=True)
    def correct_encoding(cls, value):
        return value.encode('utf8').replace(b'\x00', b'').decode("utf8")


class IncrementalCSVWriter:
    def __init__(self, filename: str, header: list[str]):
        self.filename = filename
        self.header = header
        self.count = 0
        self.current = 0
        self.file = None
        self.writer = None

    def __enter__(self):
        self.new_file()
        return self

    def new_file(self):
        self.current += 1
        self.file = open(f'./csvs/{self.filename}-{self.current:02d}.csv', 'w', newline='', encoding='utf-8') 
        self.writer = csv.writer(self.file, delimiter="|", escapechar="~")
        self.writer.writerow(self.header)
    
    def writerows(self, rows: list[list]):
        rows_len = (len(rows))
        if self.count + rows_len > 5000000:
            self.new_file()
            self.count = 0
        
        self.count += rows_len
        self.writer.writerows(rows)

    def __exit__(self, *args, **kwargs):
        self.file.close()


class DBCopier:
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/PDT')
        self.files = {}
        
        for filename in os.listdir("csvs"):
            table = filename.split('-')[0]
            if not table in self.files:
                self.files[table] = []
            self.files[table].append(filename)

    def copy_statement(self, table: str, file: str, columns: list = []) -> text:
        return text(f"""
            COPY public.{table} {"(" + ", ".join(columns) + ")" if columns else ""}
            FROM 'D:\FIIT\Inzinierske_studium\\1__zimny\PDT\Zadanie_1\csvs\{file}' 
            WITH (DELIMITER '|', ESCAPE '~', FORMAT CSV, HEADER TRUE);
        """) 

    def db_init(self):
        script = text("""
            CREATE TABLE IF NOT EXISTS public.authors
            (
                id bigint NOT NULL,
                name character varying(255) COLLATE pg_catalog."default",
                username character varying(255) COLLATE pg_catalog."default",
                description text COLLATE pg_catalog."default",
                followers_count integer,
                following_count integer,
                tweet_count integer,
                listed_count integer,
                CONSTRAINT authors_pkey PRIMARY KEY (id)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.authors
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.conversations
            (
                id bigint NOT NULL,
                author_id bigint NOT NULL,
                content text COLLATE pg_catalog."default" NOT NULL,
                possibly_sensitive boolean NOT NULL,
                language character varying(3) COLLATE pg_catalog."default" NOT NULL,
                source text COLLATE pg_catalog."default" NOT NULL,
                retweet_count integer,
                reply_count integer,
                like_count integer,
                quote_count integer,
                created_at timestamp with time zone NOT NULL,
                CONSTRAINT conversations_pkey PRIMARY KEY (id),
                CONSTRAINT author_id FOREIGN KEY (author_id)
                    REFERENCES public.authors (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.conversations
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.hashtags
            (
                id bigint NOT NULL,
                tag text COLLATE pg_catalog."default" NOT NULL UNIQUE,
                CONSTRAINT hashtags_pkey PRIMARY KEY (id)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.hashtags
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.conversation_hashtags
            (
                id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                conversation_id bigint NOT NULL,
                hashtag_id bigint NOT NULL,
                CONSTRAINT conversation_hashtags_pkey PRIMARY KEY (id),
                CONSTRAINT conversation_id FOREIGN KEY (conversation_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION,
                CONSTRAINT hashtag_id FOREIGN KEY (hashtag_id)
                    REFERENCES public.hashtags (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.conversation_hashtags
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.context_domains
            (
                id bigint NOT NULL,
                name character varying(255) COLLATE pg_catalog."default" NOT NULL,
                description text COLLATE pg_catalog."default",
                CONSTRAINT context_domains_pkey PRIMARY KEY (id)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.context_domains
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.context_entities
            (
                id bigint NOT NULL,
                name character varying(255) COLLATE pg_catalog."default" NOT NULL,
                description text COLLATE pg_catalog."default",
                CONSTRAINT context_entities_pkey PRIMARY KEY (id)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.context_entities
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.annotations
            (
                id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                conversation_id bigint NOT NULL,
                value text COLLATE pg_catalog."default" NOT NULL,
                type text COLLATE pg_catalog."default" NOT NULL,
                probability NUMERIC(4,3) NOT NULL,
                CONSTRAINT annotations_pkey PRIMARY KEY (id),
                CONSTRAINT conversation_id FOREIGN KEY (conversation_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.annotations
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.links
            (
                id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                conversation_id bigint NOT NULL,
                url character varying(2048) COLLATE pg_catalog."default" NOT NULL,
                title text COLLATE pg_catalog."default",
                description text COLLATE pg_catalog."default",
                CONSTRAINT links_pkey PRIMARY KEY (id),
                CONSTRAINT conversation_id FOREIGN KEY (conversation_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.links
                OWNER to postgres;

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.context_annotations
            (
                id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                conversation_id bigint NOT NULL,
                context_domain_id bigint NOT NULL,
                context_entity_id bigint NOT NULL,
                CONSTRAINT context_annotations_pkey PRIMARY KEY (id),
                CONSTRAINT conversation_id FOREIGN KEY (conversation_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION,
                CONSTRAINT context_domain_id FOREIGN KEY (context_domain_id)
                    REFERENCES public.context_domains (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION,
                CONSTRAINT context_entity_id FOREIGN KEY (context_entity_id)
                    REFERENCES public.context_entities (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.context_annotations
                OWNER to postgres;            

            ----------------------------------------------------------------------------------------------

            CREATE TABLE IF NOT EXISTS public.conversation_references
            (
                id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                conversation_id bigint NOT NULL,
                parent_id bigint NOT NULL,
                type character varying(20) COLLATE pg_catalog."default" NOT NULL,
                CONSTRAINT conversation_references_pkey PRIMARY KEY (id),
                CONSTRAINT conversation_id FOREIGN KEY (conversation_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION,
                CONSTRAINT parent_id FOREIGN KEY (parent_id)
                    REFERENCES public.conversations (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.conversation_references
                OWNER to postgres;
        """)

        with self.engine.begin() as transaction:
            transaction.execute(script)  

    def fill_table(self, table: str, columns: list = []):
        with self.engine.begin() as transaction:
            for file in self.files[table]:
                transaction.execute(self.copy_statement(table, file, columns))  

    def disable_triggers(self):
        script = text("""
            ALTER TABLE IF EXISTS public.authors DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversations DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.hashtags DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversation_hashtags DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_domains DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_entities DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.annotations DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.links DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_annotations DISABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversation_references DISABLE TRIGGER ALL;
        """)

        with self.engine.begin() as transaction:
            transaction.execute(script)  
    
    def enable_triggers(self):
        script = text("""
            ALTER TABLE IF EXISTS public.authors ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversations ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.hashtags ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversation_hashtags ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_domains ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_entities ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.annotations ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.links ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.context_annotations ENABLE TRIGGER ALL;
            ALTER TABLE IF EXISTS public.conversation_references ENABLE TRIGGER ALL;
        """)

        with self.engine.begin() as transaction:
            transaction.execute(script)  


def reformat_author(record: str) -> list:
    d_record = json.loads(record)
    if int(d_record["id"]) in unique_authors:
        return None

    unique_authors.add(int(d_record["id"]))

    new_record = [d_record["id"]]

    try:
        if d_record["name"] == "":
            new_record.append(None)
        else:
            new_record.append(d_record["name"].encode('utf8').replace(b'\x00', b'').decode("utf8"))
    except KeyError:
        new_record.append(None)

    try:
        if d_record["username"] == "":
            new_record.append(None)
        else:
            new_record.append(d_record["username"].encode('utf8').replace(b'\x00', b'').decode("utf8"))
    except KeyError:
        new_record.append(None)

    try:
        if d_record["description"] == "":
            new_record.append(None)
        else:
            new_record.append(d_record["description"].encode('utf8').replace(b'\x00', b'').decode("utf8"))
    except KeyError:
        new_record.append(None)

    try:
        new_record.append(d_record["public_metrics"]["followers_count"])
    except KeyError:
        new_record.append(None)
    try:
        new_record.append(d_record["public_metrics"]["following_count"])
    except KeyError:
        new_record.append(None)
    try:
        new_record.append(d_record["public_metrics"]["tweet_count"])
    except KeyError:
        new_record.append(None)
    try:
        new_record.append(d_record["public_metrics"]["listed_count"])
    except KeyError:
        new_record.append(None)

    return new_record
    
def reformat_conversation(line: str) -> tuple:
    record = Conversation.parse_obj(json.loads(line))
    
    conversation = [record.id, record.author_id, record.text, record.possibly_sensitive, record.lang, record.source]
    try:
        conversation.extend([record.public_metrics.retweet_count, record.public_metrics.reply_count, record.public_metrics.like_count, record.public_metrics.quote_count])
    except AttributeError:
        conversation.extend([None, None, None, None])
    finally:
        conversation.append(record.created_at)

    conversation_references = [[record.id, reference.id, reference.type] for reference in record.referenced_tweets]

    try:
        annotations = [[record.id, annotation.normalized_text, annotation.type, annotation.probability] for annotation in record.entities.annotations]
        
        links = [[record.id, url.expanded_url, url.title, url.description] for url in record.entities.urls if len(url.expanded_url) <= 2048]

        hashtags = []
        conversation_hashtags = []
        for hashtag in record.entities.hashtags:
            if hashtag.new:
                hashtags.append([hashtag.id, hashtag.tag])
            conversation_hashtags.append([record.id, hashtag.id])
    except AttributeError:
        annotations = []
        links = []
        hashtags = []
        conversation_hashtags = []

    context_domains = []
    context_entities = []
    context_annotations = []
    for context_annotation in record.context_annotations:
        if context_annotation.domain.new:
            context_domains. append([context_annotation.domain.id, context_annotation.domain.name, context_annotation.domain.description])
        if context_annotation.entity.new:
            context_entities.append([context_annotation.entity.id, context_annotation.entity.name, context_annotation.entity.description])
        
        context_annotations.append([record.id, context_annotation.domain.id, context_annotation.entity.id])

    
    return (
        [conversation], 
        conversation_references, 
        annotations, 
        links, 
        context_annotations, 
        context_domains,
        context_entities, 
        conversation_hashtags, 
        hashtags
    )
    
def transform_authors():
    with open("authors.jsonl", "r", encoding='utf-8') as file:
        authors_writer = csv.writer(authors_csv, delimiter="|", escapechar="~")
        
        authors_writer.writerow(["id", "name", "username", "description", "followers_count", "following_count", "tweet_count", "listed_count"])
        for line in file:
            record = reformat_author(line)
            if record:
                authors_writer.writerow(record)

def transform_conversations():
    with open("conversations.jsonl", "r", encoding='utf-8') as file:
        with (
            IncrementalCSVWriter(
                "conversations", 
                ["id", "author_id", "content", "possibly_sensitive", "language", "source", "retweet_count", "reply_count", "like_count", "quote_count", "created_at"]
            ) as conversations_writer,
            IncrementalCSVWriter(
                "conversation_references", 
                ["conversation_id", "parent_id", "type"]
            ) as conversation_references_writer,
            IncrementalCSVWriter(
                "annotations", 
                ["conversation_id", "value", "type", "probability"]
            ) as annotations_writer,
            IncrementalCSVWriter(
                "links", 
                ["conversation_id", "url", "title", "description"]
            ) as links_writer,
            IncrementalCSVWriter(
                "context_annotations", 
                ["conversation_id", "context_domain_id", "context_entity_id"]
            ) as context_annotations_writer,
            IncrementalCSVWriter(
                "context_domains", 
                ["id", "name", "description"]
            ) as context_domains_writer,
            IncrementalCSVWriter(
                "context_entities", 
                ["id", "name", "description"]
            ) as context_entities_writer,
            IncrementalCSVWriter(
                "conversation_hashtags", 
                ["conversation_id", "hashtag_id"]
            ) as conversation_hashtags_writer,
            IncrementalCSVWriter(
                "hashtags", 
                ["id", "tag"]
            ) as hashtags_writer,
        ):
            csv_writers = [
                conversations_writer,
                conversation_references_writer,
                annotations_writer,
                links_writer,
                context_annotations_writer,
                context_domains_writer,
                context_entities_writer,
                conversation_hashtags_writer,
                hashtags_writer
            ]

            for line in file:
                try:
                    for writer, table_data in zip(csv_writers, reformat_conversation(line)):
                        writer.writerows(table_data)
                except ValidationError:
                    pass

def log_block(block: str):
    global block_time
    current_time = time.time()
    
    log_writer.writerow([
        block, 
        datetime.datetime.now().strftime("%Y-%m-%dT%H:%MZ"), 
        f"{int((start_time - current_time)/60)}:{int((start_time - current_time)%60):02d}", 
        f"{int((block_time - current_time)/60)}:{int((block_time - current_time)%60):02d}"
    ])

    block_time = current_time


transform_authors()
log_block("authors.jsonl conversion")

transform_conversations()
log_block("conversations.jsonl conversion")

authors_csv.close()
unique_hashtags.clear()
unique_domains.clear()
unique_entities.clear()
unique_conversations.clear()
unique_authors.clear()


copier = DBCopier()
copier.db_init()
log_block("database initialization")

copier.disable_triggers()
log_block("disabling triggers")


copier.fill_table('hashtags')
log_block("table: hashtags")

copier.fill_table('context_domains')
log_block("table: context_domains")

copier.fill_table('context_entities')
log_block("table: context_entities")

copier.fill_table('authors')
log_block("table: authors")

copier.fill_table('conversations')
log_block("table: conversations")

copier.fill_table('context_annotations', ["conversation_id", "context_domain_id", "context_entity_id"])
log_block("table: context_annotations")

copier.fill_table('annotations', ["conversation_id", "value", "type", "probability"])
log_block("table: annotations")

copier.fill_table('links', ["conversation_id", "url", "title", "description"])
log_block("table: links")

copier.fill_table('conversation_hashtags', ["conversation_id", "hashtag_id"])
log_block("table: conversation_hashtags")

with copier.engine.begin() as transaction:
    transaction.execute(text("""
        CREATE TABLE IF NOT EXISTS public._conversation_references
        (
            conversation_id bigint NOT NULL,
            parent_id bigint NOT NULL,
            type character varying(20) COLLATE pg_catalog."default" NOT NULL
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public._conversation_references
            OWNER to postgres;
    """))

    for file in copier.files["conversation_references"]:    
        transaction.execute(copier.copy_statement("_conversation_references", file, ["conversation_id", "parent_id", "type"]))

    transaction.execute(text("""
        INSERT INTO public.conversation_references (conversation_id, parent_id, type)
        SELECT _conversation_references.* FROM public._conversation_references 
        JOIN public.conversations AS conversations_1 ON _conversation_references.conversation_id = conversations_1.id
        JOIN public.conversations AS conversations_2 ON _conversation_references.parent_id = conversations_2.id;
    """))

    transaction.execute(text("""
        DROP TABLE IF EXISTS public._conversation_references
    """))

log_block("table: conversation_references")

copier.enable_triggers()
log_block("enabling triggers")

log_csv.close()
