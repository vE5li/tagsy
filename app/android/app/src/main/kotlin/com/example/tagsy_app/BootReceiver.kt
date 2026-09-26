/*
 * Auto-start: brings up [TagsyService] (and with it the native runtime)
 * after the device boots, and after the app is updated (an update kills the
 * process, foreground service included), without the user opening the app.
 *
 * Gated on "All files access" exactly like MainActivity.maybeStartRuntime:
 * without it the engine can't create the shared-storage sync directory. A
 * receiver can't show the settings screen, so if the grant is missing we do
 * nothing; the next time the user opens the app, MainActivity asks for it.
 *
 * BOOT_COMPLETED is delivered only after the first unlock, so `filesDir`
 * (credential-encrypted storage, where the identity and databases live) is
 * readable by then.
 */

package com.example.tagsy_app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Environment
import android.util.Log

class BootReceiver : BroadcastReceiver() {

    companion object {
        private const val TAG = "tagsy"
    }

    override fun onReceive(context: Context, intent: Intent) {
        when (intent.action) {
            Intent.ACTION_BOOT_COMPLETED,
            Intent.ACTION_MY_PACKAGE_REPLACED -> Unit
            else -> return
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R &&
            !Environment.isExternalStorageManager()
        ) {
            Log.i(TAG, "BootReceiver: no All files access yet; not auto-starting")
            return
        }

        Log.i(TAG, "BootReceiver: starting TagsyService (${intent.action})")
        try {
            context.startForegroundService(Intent(context, TagsyService::class.java))
        } catch (error: Exception) {
            // e.g. ForegroundServiceStartNotAllowedException if the OS refuses;
            // the user opening the app starts it anyway.
            Log.e(TAG, "BootReceiver: could not start TagsyService: $error")
        }
    }
}
