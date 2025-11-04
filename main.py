# File: main.py
# Author: Joseph Taylor
# Date: 11/02/2025
# Course: CS 493 - Cloud Application Development
# Project 3 - Implementing a REST API Using MySQL

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import os

from flask import Flask, request, url_for
import sqlalchemy
from sqlalchemy.exc import IntegrityError

from connect_connector import connect_with_connector

# ===================
# ==== Constants ====
# ===================

BUSINESSES = 'businesses'
REVIEWS = 'reviews'

# Field Requirements
REQUIRED_BUSINESS_FIELDS = {
    "owner_id", "name", "street_address", "city", "state", "zip_code"
}
REQUIRED_REVIEW_FIELDS = {
    "user_id", "business_id", "stars"
}

# Fields including Non-Requirementals
REVIEW_FIELDS = REQUIRED_REVIEW_FIELDS.union({"review_text"})

ERROR_BUSINESS_NOT_FOUND = {'Error': 'No business with this business_id exists'}
ERROR_REVIEW_NOT_FOUND = {'Error': 'No review with this review_id exists'}

app = Flask(__name__)
logger = logging.getLogger(__name__)

# ==========================
# ==== DB Initialization ===
# ==========================

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

def create_tables(db: sqlalchemy.engine.base.Engine) -> None:
    """
    Create the required tables if they don't already exist.
    Enforces:
      - FK reviews.business_id -> businesses.business_id ON DELETE CASCADE
      - UNIQUE (user_id, business_id) on reviews
      - CHECK stars between 1 and 5
    """
    with db.connect() as conn:
        # businesses
        conn.execute(sqlalchemy.text(
            '''
            CREATE TABLE IF NOT EXISTS businesses (
              business_id     INT AUTO_INCREMENT PRIMARY KEY,
              owner_id        INT NOT NULL,
              name            VARCHAR(50) NOT NULL,
              street_address  VARCHAR(100) NOT NULL,
              city            VARCHAR(50) NOT NULL,
              state           CHAR(2) NOT NULL,
              zip_code        CHAR(5) NOT NULL
            ) ENGINE=InnoDB;
            '''
        ))

        # reviews
        conn.execute(sqlalchemy.text(
            '''
            CREATE TABLE IF NOT EXISTS reviews (
            review_id     INT AUTO_INCREMENT PRIMARY KEY,
            user_id       INT NOT NULL,
            business_id   INT NOT NULL,
            stars         INT NOT NULL,
            review_text   VARCHAR(1000) NOT NULL DEFAULT '',
            CONSTRAINT fk_reviews_business
                FOREIGN KEY (business_id)
                REFERENCES businesses(business_id)
                ON DELETE CASCADE,
            CONSTRAINT uq_user_business UNIQUE (user_id, business_id),
            CONSTRAINT ck_stars CHECK (stars BETWEEN 0 AND 5)
            ) ENGINE=InnoDB;
            '''
        ))
        conn.commit()

# ==========================
# ==== Helper functions ====
# ==========================

def bad_request(message: str, status: int = 400):
    return {'Error': message}, status

def has_required_fields(data: dict, required_fields: set[str]):
    for field in required_fields:
        if field not in data:
            return field
    return None

def row_to_business_dict(row) -> dict:
    d = row._asdict()
    out = {
        "id": d["business_id"],
        "owner_id": d["owner_id"],
        "name": d["name"],
        "street_address": d["street_address"],
        "city": d["city"],
        "state": d["state"],
        "zip_code": int(d["zip_code"]),
        "self": url_for('get_business_by_id', business_id=d["business_id"], _external=True, _scheme='https')
    }
    return out

def row_to_review_dict(row) -> dict:
    d = row._asdict()
    review_id = d["review_id"]
    business_id = d["business_id"]
    out = {
        "id": review_id,
        "user_id": d["user_id"],
        "business": url_for('get_business_by_id', business_id=business_id, _external=True, _scheme='https'),
        "stars": d["stars"],
        "review_text": d["review_text"] if d["review_text"] is not None else "",
        "self": url_for('get_review_by_id', review_id=review_id, _external=True, _scheme='https')
    }
    return out

# ==============================

@app.route('/')
def index():
    return 'Please access either the /businesses or /reviews endpoint.'

# ==============================
# ==== Businesses Endpoints ====
# ==============================

# Post Business
@app.route('/' + BUSINESSES, methods=['POST'])
def post_business():
    content = request.get_json()
    missing = has_required_fields(content, REQUIRED_BUSINESS_FIELDS)
    if missing:
        return bad_request("The request body is missing at least one of the required attributes")

    try:
        with db.connect() as conn:
            # Prepare and execute INSERT
            stmt = sqlalchemy.text(
                '''
                INSERT INTO businesses (owner_id, name, street_address, city, state, zip_code)
                VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)
                '''
            )
            conn.execute(stmt, parameters={
                "owner_id": content["owner_id"],
                "name": content["name"],
                "street_address": content["street_address"],
                "city": content["city"],
                "state": content["state"],
                "zip_code": content["zip_code"],
            })

            # Get new id and commit
            new_id = conn.execute(sqlalchemy.text('SELECT LAST_INSERT_ID()')).scalar()
            conn.commit()

            # Fetch the created row, add self URL
            row = conn.execute(sqlalchemy.text(
                '''
                SELECT business_id, owner_id, name, street_address, city, state, zip_code
                FROM businesses WHERE business_id = :id
                '''
            ), {"id": new_id}).one()

    except Exception as e:
        logger.exception(e)
        return {"Error": "Unable to create business"}, 500

    return row_to_business_dict(row), 201

# Get All Businesses (paginated by 3)
@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    # If params are absent, return the first page
    limit = request.args.get('limit', type=int)
    offset = request.args.get('offset', type=int)
    if limit is None or offset is None:
        limit, offset = 3, 0

    # Fetch one extra row to know if a "next" page exists
    fetch_limit = max(0, limit) + 1
    offset = max(0, offset)

    with db.connect() as conn:
        rows = list(conn.execute(sqlalchemy.text(
            '''
            SELECT business_id, owner_id, name, street_address, city, state, zip_code
            FROM businesses
            ORDER BY business_id
            LIMIT :limit OFFSET :offset
            '''
        ), {"limit": fetch_limit, "offset": offset}))

    # Build current page entries
    entries = [row_to_business_dict(r) for r in rows[:limit]]
    body = {"entries": entries}

    # Add `next` only if there are more than `limit` rows
    if len(rows) > limit:
        body["next"] = url_for('get_businesses',
                               offset=offset + limit,
                               limit=limit,
                               _external=True, _scheme='https')
    return body, 200

# Get Business by ID
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['GET'])
def get_business_by_id(business_id):
    with db.connect() as conn:
        row = conn.execute(sqlalchemy.text(
            '''
            SELECT business_id, owner_id, name, street_address, city, state, zip_code
            FROM businesses WHERE business_id = :id
            '''
        ), {"id": business_id}).one_or_none()
        if row is None:
            return ERROR_BUSINESS_NOT_FOUND, 404
        return row_to_business_dict(row), 200

# Get All Businesses by Owner ID
@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_businesses_for_owner(owner_id):
    with db.connect() as conn:
        rows = conn.execute(sqlalchemy.text(
            '''
            SELECT business_id, owner_id, name, street_address, city, state, zip_code
            FROM businesses
            WHERE owner_id = :owner_id
            ORDER BY business_id
            '''
        ), {"owner_id": owner_id})
        return [row_to_business_dict(r) for r in rows], 200

# Edit Business by ID
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    content = request.get_json()
    missing = has_required_fields(content, REQUIRED_BUSINESS_FIELDS)
    if missing:
        return bad_request("The request body is missing at least one of the required attributes")

    with db.connect() as conn:
        # Ensure business exists
        existing = conn.execute(sqlalchemy.text(
            'SELECT business_id FROM businesses WHERE business_id = :id'
        ), {"id": business_id}).one_or_none()
        if existing is None:
            return ERROR_BUSINESS_NOT_FOUND, 404

        conn.execute(sqlalchemy.text(
            '''
            UPDATE businesses
            SET owner_id=:owner_id, name=:name, street_address=:street_address,
                city=:city, state=:state, zip_code=:zip_code
            WHERE business_id=:id
            '''
        ), {
            "owner_id": content["owner_id"],
            "name": content["name"],
            "street_address": content["street_address"],
            "city": content["city"],
            "state": content["state"],
            "zip_code": content["zip_code"],
            "id": business_id
        })
        conn.commit()

        row = conn.execute(sqlalchemy.text(
            '''
            SELECT business_id, owner_id, name, street_address, city, state, zip_code
            FROM businesses WHERE business_id = :id
            '''
        ), {"id": business_id}).one()
        return row_to_business_dict(row), 200

# Delete Business (and its Reviews)
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    with db.connect() as conn:
        # Deleting the business will cascade delete its reviews (FK ON DELETE CASCADE)
        result = conn.execute(sqlalchemy.text(
            'DELETE FROM businesses WHERE business_id = :id'
        ), {"id": business_id})
        conn.commit()
        if result.rowcount == 0:
            return ERROR_BUSINESS_NOT_FOUND, 404
        return '', 204

# ===========================
# ==== Reviews Endpoints ====
# ===========================

# Post Review (enforce one per user per business)
@app.route('/' + REVIEWS, methods=['POST'])
def post_reviews():
    content = request.get_json()

    # Required fields
    missing = has_required_fields(content, REQUIRED_REVIEW_FIELDS)
    if missing:
        return bad_request("The request body is missing at least one of the required attributes")

    try:
        with db.connect() as conn:
            # Validate the business exists (404 if not)
            b = conn.execute(
                sqlalchemy.text('SELECT business_id FROM businesses WHERE business_id = :bid'),
                {"bid": content["business_id"]}
            ).one_or_none()
            if b is None:
                return ERROR_BUSINESS_NOT_FOUND, 404

            # Insert review
            stmt = sqlalchemy.text(
                '''
                INSERT INTO reviews (user_id, business_id, stars, review_text)
                VALUES (:user_id, :business_id, :stars, :review_text)
                '''
            )
            conn.execute(stmt, {
                "user_id": content["user_id"],
                "business_id": content["business_id"],
                "stars": content["stars"],
                "review_text": content.get("review_text", "")
            })

            # Fetch new id and commit
            new_id = conn.execute(sqlalchemy.text('SELECT LAST_INSERT_ID()')).scalar()
            conn.commit()

            # Fetch the created row, add self URL
            row = conn.execute(sqlalchemy.text(
                '''
                SELECT review_id, user_id, business_id, stars, review_text
                FROM reviews WHERE review_id = :id
                '''
            ), {"id": new_id}).one()

    except IntegrityError as e:
        # Unique (user_id, business_id) - 409 Error
        msg = str(e.orig).lower()
        if 'uq_user_business' in msg or 'duplicate' in msg or 'unique' in msg:
            return bad_request(
                "You have already submitted a review for this business. "
                "You can update your previous review, or delete it and submit a new review",
                status=409
            )
        # Other integrity issues (CHECK stars 0–5, etc) - 400 Error
        return bad_request("Invalid review data")
    
    except Exception as e:
        logger.exception(e)
        return {"Error": "Unable to create review"}, 500

    return row_to_review_dict(row), 201

# Get Review by ID
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['GET'])
def get_review_by_id(review_id):
    with db.connect() as conn:
        row = conn.execute(sqlalchemy.text(
            '''
            SELECT review_id, user_id, business_id, stars, review_text
            FROM reviews WHERE review_id = :id
            '''
        ), {"id": review_id}).one_or_none()
        if row is None:
            return ERROR_REVIEW_NOT_FOUND, 404
        return row_to_review_dict(row), 200

# List all Reviews by a User ID
@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_reviews_for_user(user_id):
    with db.connect() as conn:
        rows = conn.execute(sqlalchemy.text(
            '''
            SELECT review_id, user_id, business_id, stars, review_text
            FROM reviews WHERE user_id = :uid
            ORDER BY review_id
            '''
        ), {"uid": user_id})
        return [row_to_review_dict(r) for r in rows], 200

# Edit a Review by ID (stars required; review_text optional)
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    content = request.get_json(silent=True) or {}
    if "stars" not in content:
        return bad_request("The request body is missing at least one of the required attributes")

    with db.connect() as conn:
        exists = conn.execute(sqlalchemy.text(
            'SELECT review_id FROM reviews WHERE review_id = :id'
        ), {"id": review_id}).one_or_none()
        if exists is None:
            return ERROR_REVIEW_NOT_FOUND, 404

        # Allow optional review_text
        if "review_text" in content:
            stmt = sqlalchemy.text(
                '''
                UPDATE reviews
                SET stars = :stars, review_text = :review_text
                WHERE review_id = :id
                '''
            )
            params = {"stars": content["stars"], "review_text": content["review_text"], "id": review_id}
        else:
            stmt = sqlalchemy.text(
                'UPDATE reviews SET stars = :stars WHERE review_id = :id'
            )
            params = {"stars": content["stars"], "id": review_id}

        try:
            conn.execute(stmt, params)
            conn.commit()
        except IntegrityError:
            return bad_request("Invalid review data")

        row = conn.execute(sqlalchemy.text(
            '''
            SELECT review_id, user_id, business_id, stars, review_text
            FROM reviews WHERE review_id = :id
            '''
        ), {"id": review_id}).one()
        return row_to_review_dict(row), 200

# Delete a Review by ID
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    with db.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            'DELETE FROM reviews WHERE review_id = :id'
        ), {"id": review_id})
        conn.commit()
        if result.rowcount == 0:
            return ERROR_REVIEW_NOT_FOUND, 404
        return '', 204

# ================
# ==== Main ======
# ================


init_db()
create_tables(db)
