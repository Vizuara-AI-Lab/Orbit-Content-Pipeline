#!/usr/bin/env python3
"""
copy_pipeline_state.py — Copies _PipelineStateProd from Orbit Firestore → Prod Firestore.

Copies every course-level document and its Lessons subcollection documents.

Usage:
    python copy_pipeline_state.py
"""

import os
import firebase_admin
from firebase_admin import credentials, firestore

_orbit_app = firebase_admin.initialize_app(
    credentials.Certificate(os.getenv("ORBIT_SERVICE_ACCOUNT", "orbit-service-account.json")),
    name="orbit",
)
_prod_app = firebase_admin.initialize_app(
    credentials.Certificate(os.getenv("PROD_SERVICE_ACCOUNT", "prod-service-account.json")),
    name="prod",
)

orbit_db = firestore.client(app=_orbit_app)
prod_db   = firestore.client(app=_prod_app)

COLLECTION = "_PipelineStateProd"


def copy_collection():
    course_docs = list(orbit_db.collection(COLLECTION).stream())
    print(f"Found {len(course_docs)} course document(s) in Orbit {COLLECTION}\n")

    for course_doc in course_docs:
        course_id = course_doc.id
        data = course_doc.to_dict() or {}

        prod_db.collection(COLLECTION).document(course_id).set(data)
        print(f"  Copied course: {course_id}")

        lesson_docs = list(
            orbit_db.collection(COLLECTION)
            .document(course_id)
            .collection("Lessons")
            .stream()
        )

        for lesson_doc in lesson_docs:
            lesson_data = lesson_doc.to_dict() or {}
            (
                prod_db.collection(COLLECTION)
                .document(course_id)
                .collection("Lessons")
                .document(lesson_doc.id)
                .set(lesson_data)
            )

        print(f"    → {len(lesson_docs)} lesson(s) copied")

    print(f"\nDone. {len(course_docs)} course(s) copied.")


if __name__ == "__main__":
    copy_collection()
