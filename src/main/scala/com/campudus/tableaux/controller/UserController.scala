package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.UserModel
import com.campudus.tableaux.database.domain.UserSettingGlobalSeq
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.verticles.EventClient
import com.campudus.tableaux.router.auth.permission.TableauxUser

import scala.concurrent.Future

object UserController {

  def apply(
      config: TableauxConfig,
      repository: UserModel,
      roleModel: RoleModel
  ): UserController = {
    new UserController(
      config,
      repository,
      roleModel
    )
  }
}

class UserController(
    override val config: TableauxConfig,
    override protected val repository: UserModel,
    implicit protected val roleModel: RoleModel
) extends Controller[UserModel] {
  val eventClient: EventClient = EventClient(vertx)

    def retrieveGlobalSettings()(implicit user: TableauxUser): Future[UserSettingGlobalSeq] = {
    logger.info(s"retrieveGlobalSettings user: ${user.name}")

    for {
      globalSettingsSeq <- repository.retrieveGlobalSettings()
    } yield {
      UserSettingGlobalSeq(globalSettingsSeq)
    }
  }
}
