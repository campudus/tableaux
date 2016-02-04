package com.campudus.tableaux.database

sealed trait ReturnType

case object GetReturn extends ReturnType

case object EmptyReturn extends ReturnType