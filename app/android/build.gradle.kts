allprojects {
    repositories {
        google()
        mavenCentral()
    }
}

val newBuildDir: Directory =
    rootProject.layout.buildDirectory
        .dir("../../build")
        .get()
rootProject.layout.buildDirectory.value(newBuildDir)

subprojects {
    val newSubprojectBuildDir: Directory = newBuildDir.dir(project.name)
    project.layout.buildDirectory.value(newSubprojectBuildDir)
}
subprojects {
    // Force plugin subprojects to compile against the SDK platform this
    // (read-only, Nix-provided) Android SDK actually ships (35/36), overriding
    // any lower compileSdk they declare. Without this, a plugin pinned to e.g.
    // compileSdk 34 (receive_sharing_intent 1.8.x) makes Gradle try to
    // auto-install `platforms;android-34` into the immutable Nix store and fail
    // with "The SDK directory is not writable". compileSdk is backward
    // compatible, so bumping it up is safe. Only touches subprojects that
    // declare a lower value.
    //
    // Registered BEFORE evaluationDependsOn(":app") below, which forces
    // subproject evaluation: an afterEvaluate added after evaluation has begun
    // throws "Cannot run Project.afterEvaluate when the project is already
    // evaluated".
    afterEvaluate {
        extensions.findByName("android")?.let { ext ->
            val android = ext as com.android.build.gradle.BaseExtension
            val current = android.compileSdkVersion?.removePrefix("android-")?.toIntOrNull()
            if (current != null && current < 36) {
                android.compileSdkVersion(36)
            }
            // Older plugins (e.g. receive_sharing_intent 1.8.x) default their
            // Java compile to 1.8 while the toolchain's Kotlin compiles to 17,
            // which fails with "Inconsistent JVM-target compatibility". Pin both
            // to 17 to match the app module (build.gradle.kts).
            android.compileOptions {
                sourceCompatibility = JavaVersion.VERSION_17
                targetCompatibility = JavaVersion.VERSION_17
            }
        }
        tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
            compilerOptions {
                jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
            }
        }
    }
    project.evaluationDependsOn(":app")
}

tasks.register<Delete>("clean") {
    delete(rootProject.layout.buildDirectory)
}
