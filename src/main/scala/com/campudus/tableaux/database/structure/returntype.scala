package com.campudus.tableaux.database.structure

sealed trait ReturnType

case object GetReturn extends ReturnType

case object SetReturn extends ReturnType

case object EmptyReturn extends ReturnType