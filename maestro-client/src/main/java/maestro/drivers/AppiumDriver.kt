package maestro.drivers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dadb.AdbShellPacket
import dadb.AdbShellStream
import io.appium.java_client.android.geolocation.AndroidGeoLocation
import io.grpc.Status
import io.grpc.StatusRuntimeException
//import io.micrometer.core.instrument.cumulative.CumulativeCounter
//import io.micrometer.core.instrument.cumulative.CumulativeTimer
import maestro.Capability
import maestro.DeviceInfo
import maestro.Driver
import maestro.KeyCode
import maestro.Maestro
import maestro.MaestroException
import maestro.NamedSource
import maestro.Platform
import maestro.Point
import maestro.ScreenRecording
import maestro.SwipeDirection
import maestro.TreeNode
import maestro.ViewHierarchy
import maestro.filterOutOfBounds
import maestro.utils.Metrics
import maestro.utils.MetricsProvider
import maestro.utils.ScreenshotUtils
import maestro_android.MaestroAndroid
import okio.Sink
import okio.buffer
import okio.sink
import okio.source
import org.openqa.selenium.OutputType
import org.openqa.selenium.WebElement
import org.openqa.selenium.interactions.Pause
import org.openqa.selenium.interactions.PointerInput
import org.openqa.selenium.interactions.Sequence
import org.openqa.selenium.remote.SessionId
import org.slf4j.LoggerFactory
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.StringWriter
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

private val logger = LoggerFactory.getLogger(Maestro::class.java)


class AppiumDriver(
    private val appiumDriver: io.appium.java_client.AppiumDriver,
    private val emulatorName: String = "",
    private val metricsProvider: Metrics = MetricsProvider.getInstance(),
) : Driver {
    private var open = false

    private val metrics = metricsProvider.withPrefix("maestro.driver")
        .withTags(mapOf("platform" to "appium", "emulatorName" to emulatorName))

    private val documentBuilderFactory = DocumentBuilderFactory.newInstance()

    private var proxySet = false
    private var closed = false

    private var chromeDevToolsEnabled = false

    private var appiumSession: SessionId? = null


    override fun name(): String {
        return "Appium Device ($appiumDriver) ($emulatorName)"
    }

    override fun open() {
//        allocateForwarder()
//        installMaestroApks()
//        startInstrumentationSession(hostPort)

//        try {
//            awaitLaunch()
//        } catch (ignored: InterruptedException) {
//            instrumentationSession?.close()
//            return
//        }
        appiumSession = appiumDriver.sessionId
    }

    private fun getDeviceApiLevel(): Int {
        return appiumDriver.capabilities.getCapability("platformVersion") as Int
    }

//    private fun awaitLaunch() {
//        val startTime = System.currentTimeMillis()
//
//        while (System.currentTimeMillis() - startTime < getStartupTimeout()) {
//            runCatching {
//                dadb.open("tcp:$hostPort").close()
//                return
//            }
//            Thread.sleep(100)
//        }
//
//        throw MaestroDriverStartupException.AndroidDriverTimeoutException("Maestro Android driver did not start up in time  ---  emulator [ ${emulatorName} ] & port  [ dadb.open( tcp:${hostPort} ) ]")
//    }

    override fun close() {
//        if (false) {
//            val meters = metrics.registry.meters
//
//            for (meter in meters) {
//                println(meter.id.tags[0])
//                try {
//                    println((meter as CumulativeTimer).count())
//                } catch (e: Exception) {
//                    println((meter as CumulativeCounter).count())
//                }
//            }
//        }
        if (closed) return
        if (proxySet) {
            resetProxy()
        }
        appiumDriver.quit()
    }

    override fun deviceInfo(): DeviceInfo {
        val platform = appiumDriver.capabilities.platformName.toString()
        val screenSize = appiumDriver.manage().window().size
        val widthPixels = screenSize.width
        val heightPixels = screenSize.height

        return runDeviceCall {
            DeviceInfo(
                platform = Platform.valueOf(platform),
                widthPixels = widthPixels,
                heightPixels = heightPixels,
                widthGrid = widthPixels,
                heightGrid = heightPixels,
            )
        }
    }

    override fun launchApp(
        appId: String,
        launchArguments: Map<String, Any>,
    ) {
        metrics.measured("operation", mapOf("command" to "launchApp", "appId" to appId)) {
            if (!open) // pick device flow, no open() invocation
                open()

            if (!isPackageInstalled(appId)) {
                throw IllegalArgumentException("Package $appId is not installed")
            }

//            val arguments = launchArguments.toAndroidLaunchArguments()
            runDeviceCall {
                if (appiumDriver is io.appium.java_client.android.AndroidDriver) {
                    appiumDriver.activateApp(appId)
                }
            }
        }
    }

    override fun stopApp(appId: String) {
        metrics.measured("operation", mapOf("command" to "stopApp", "appId" to appId)) {
            // Note: If the package does not exist, this call does *not* throw an exception
            if (appiumDriver is io.appium.java_client.android.AndroidDriver) {
                appiumDriver.terminateApp(appId)
            }
        }
    }

    override fun killApp(appId: String) {
        metrics.measured("operation", mapOf("command" to "killApp", "appId" to appId)) {
            // Kill is the adb command needed to trigger System-initiated Process Death
            shell("am kill $appId")
        }
    }

    override fun clearAppState(appId: String) {
        metrics.measured("operation", mapOf("command" to "clearAppState", "appId" to appId)) {
            if (!isPackageInstalled(appId)) {
                return@measured
            }

            shell("pm clear $appId")
        }
    }

    override fun clearKeychain() {
        // No op
    }

    override fun tap(point: Point) {
        metrics.measured("operation", mapOf("command" to "tap")) {
            val finger = PointerInput(PointerInput.Kind.TOUCH, "finger")
            val sequence: Sequence = Sequence(finger, 1)
                .addAction(
                    finger.createPointerMove(
                        Duration.ZERO,
                        PointerInput.Origin.viewport(),
                        point.x,
                        point.y
                    )
                )
                .addAction(finger.createPointerDown(PointerInput.MouseButton.LEFT.asArg()))
                .addAction(Pause(finger, Duration.ofMillis(150)))
                .addAction(finger.createPointerUp(PointerInput.MouseButton.LEFT.asArg()))
            appiumDriver.perform(Collections.singletonList(sequence))
        }
    }

    override fun longPress(point: Point) {
        metrics.measured("operation", mapOf("command" to "longPress")) {
            swipe(point, point, 3000)
        }
    }

    override fun pressKey(code: KeyCode) {
        metrics.measured("operation", mapOf("command" to "pressKey")) {
            val intCode: Int = when (code) {
                KeyCode.ENTER -> 66
                KeyCode.BACKSPACE -> 67
                KeyCode.BACK -> 4
                KeyCode.VOLUME_UP -> 24
                KeyCode.VOLUME_DOWN -> 25
                KeyCode.HOME -> 3
                KeyCode.LOCK -> 276
                KeyCode.REMOTE_UP -> 19
                KeyCode.REMOTE_DOWN -> 20
                KeyCode.REMOTE_LEFT -> 21
                KeyCode.REMOTE_RIGHT -> 22
                KeyCode.REMOTE_CENTER -> 23
                KeyCode.REMOTE_PLAY_PAUSE -> 85
                KeyCode.REMOTE_STOP -> 86
                KeyCode.REMOTE_NEXT -> 87
                KeyCode.REMOTE_PREVIOUS -> 88
                KeyCode.REMOTE_REWIND -> 89
                KeyCode.REMOTE_FAST_FORWARD -> 90
                KeyCode.POWER -> 26
                KeyCode.ESCAPE -> 111
                KeyCode.TAB -> 62
                KeyCode.REMOTE_SYSTEM_NAVIGATION_UP -> 280
                KeyCode.REMOTE_SYSTEM_NAVIGATION_DOWN -> 281
                KeyCode.REMOTE_BUTTON_A -> 96
                KeyCode.REMOTE_BUTTON_B -> 97
                KeyCode.REMOTE_MENU -> 82
                KeyCode.TV_INPUT -> 178
                KeyCode.TV_INPUT_HDMI_1 -> 243
                KeyCode.TV_INPUT_HDMI_2 -> 244
                KeyCode.TV_INPUT_HDMI_3 -> 245
            }

            shell("input keyevent $intCode")
            Thread.sleep(300)
        }
    }

    override fun contentDescriptor(excludeKeyboardElements: Boolean): TreeNode {
        return metrics.measured("operation", mapOf("command" to "contentDescriptor")) {

            val response = getHierarchyFromAppium()
            val document = documentBuilderFactory
                .newDocumentBuilder()
                .parse(response.hierarchy.byteInputStream())

            val baseTree = mapHierarchy(document)
            val treeNode = baseTree

//            val treeNode: TreeNode = androidWebViewHierarchyClient.augmentHierarchy(baseTree, chromeDevToolsEnabled)

            if (excludeKeyboardElements) {
                treeNode.excludeKeyboardElements() ?: treeNode
            } else {
                treeNode
            }

        }
    }

    private fun getHierarchyFromAppium(): MaestroAndroid.ViewHierarchyResponse {
        val pageSource = appiumDriver.pageSource
            ?: throw IllegalStateException("Appium driver failed to get page source")
        val hierarchy = convertAppiumXmlToMaestroXml(pageSource)
        val response = MaestroAndroid.ViewHierarchyResponse.newBuilder()
            .setHierarchy(hierarchy)
            .build()
        return response
    }

    @Throws(java.lang.Exception::class)
    fun convertAppiumXmlToMaestroXml(appiumXml: String): String {
        val dbFactory = DocumentBuilderFactory.newInstance()
        val dBuilder = dbFactory.newDocumentBuilder()
        val appiumDoc =
            dBuilder.parse(ByteArrayInputStream(appiumXml.toByteArray(StandardCharsets.UTF_8)))
        appiumDoc.documentElement.normalize()

        // Create a new Document for the Maestro-style XML
        val maestroDoc = dBuilder.newDocument()
        val rootElement = maestroDoc.createElement("hierarchy")
        rootElement.setAttribute("rotation", "0") // Maestro's root attribute
        maestroDoc.appendChild(rootElement)

        // Process each Appium node and create a Maestro node
        val appiumNodes = appiumDoc.childNodes.item(0).childNodes // Get children of hierarchy
        for (i in 0 until appiumNodes.length) {
            val appiumNode = appiumNodes.item(i)
            if (appiumNode.nodeType == Node.ELEMENT_NODE) {
                val appiumElement = appiumNode as Element
                val maestroNode = maestroDoc.createElement("node")

                copyAttributes(appiumElement, maestroNode) // Use the helper method

                rootElement.appendChild(maestroNode)

                // Recursively process child nodes (if any)
                processChildNodes(appiumElement, maestroNode, maestroDoc)
            }
        }

        // Convert the Document to a String
        val tf = TransformerFactory.newInstance()
        val transformer = tf.newTransformer()
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes") // Omit <?xml ... ?>
        transformer.setOutputProperty(OutputKeys.INDENT, "yes") // Add indentation for readability
        val writer = StringWriter()
        transformer.transform(DOMSource(maestroDoc), StreamResult(writer))
        return writer.toString()
    }

    private fun copyAttributes(appiumElement: Element, maestroElement: Element) {
        // Define all the attributes we want to copy
        val attributes = arrayOf(
            "index", "text", "resource-id", "class", "package", "content-desc",
            "checkable", "checked", "clickable", "enabled", "focusable", "focused",
            "scrollable", "long-clickable", "password", "selected", "bounds", "displayed",
            "label", "name"
        )

        for (attr in attributes) {
            val value = appiumElement.getAttribute(attr)
            if (value.isNotEmpty()) { // Check for null and empty
                maestroElement.setAttribute(attr, value)
            } else {
                maestroElement.setAttribute(attr, "") // Default to empty
            }
        }

        // Map 'displayed' to 'visible-to-user'
        val displayed = appiumElement.getAttribute("displayed").takeUnless { it.isNullOrEmpty() }
            ?: appiumElement.getAttribute("visible")
        maestroElement.setAttribute("visible-to-user", displayed)

        // Add empty hintText
        maestroElement.setAttribute("hintText", "")
    }

    private fun processChildNodes(
        appiumParent: Element,
        maestroParent: Element,
        maestroDoc: Document
    ) {
        val appiumChildren = appiumParent.childNodes
        for (i in 0 until appiumChildren.length) {
            val appiumChild = appiumChildren.item(i)
            if (appiumChild.nodeType == Node.ELEMENT_NODE) {
                val appiumChildElement = appiumChild as Element
                val maestroChild = maestroDoc.createElement("node")

                copyAttributes(appiumChildElement, maestroChild)

                maestroParent.appendChild(maestroChild)
                processChildNodes(appiumChildElement, maestroChild, maestroDoc)
            }
        }
    }

    private fun TreeNode.excludeKeyboardElements(): TreeNode? {
        val filtered = children.mapNotNull {
            it.excludeKeyboardElements()
        }.toList()

        val resourceId = attributes["resource-id"]
        if (resourceId != null && resourceId.startsWith("com.google.android.inputmethod.latin:id/")) {
            return null
        }
        return TreeNode(
            attributes = attributes,
            children = filtered,
            clickable = clickable,
            enabled = enabled,
            focused = focused,
            checked = checked,
            selected = selected
        )
    }

    override fun scrollVertical() {
        metrics.measured("operation", mapOf("command" to "scrollVertical")) {
            swipe(SwipeDirection.UP, 400)
        }
    }

    override fun isKeyboardVisible(): Boolean {
        return metrics.measured("operation", mapOf("command" to "isKeyboardVisible")) {
            val root = contentDescriptor().let {
                val deviceInfo = deviceInfo()
                val filtered = it.filterOutOfBounds(
                    width = deviceInfo.widthGrid,
                    height = deviceInfo.heightGrid
                )
                filtered ?: it
            }
            "com.google.android.inputmethod.latin:id" in jacksonObjectMapper().writeValueAsString(
                root
            )
        }
    }

    override fun swipe(start: Point, end: Point, durationMs: Long) {
        val finger = PointerInput(PointerInput.Kind.TOUCH, "finger1")
        val sequence = Sequence(finger, 1)

        // Action 1: Move finger to start point
        sequence.addAction(
            finger.createPointerMove(
                Duration.ZERO,
                PointerInput.Origin.viewport(),
                start.x,
                start.y
            )
        )
        // Action 2: Press down
        sequence.addAction(finger.createPointerDown(PointerInput.MouseButton.LEFT.asArg()))
        // Action 3: Move finger to end point over specified duration
        sequence.addAction(
            finger.createPointerMove(
                Duration.ofMillis(durationMs),
                PointerInput.Origin.viewport(),
                end.x,
                end.y
            )
        )
        // Action 4: Release finger
        sequence.addAction(finger.createPointerUp(PointerInput.MouseButton.LEFT.asArg()))

        appiumDriver.perform(listOf(sequence))
    }

    override fun swipe(swipeDirection: SwipeDirection, durationMs: Long) {
        metrics.measured(
            "operation",
            mapOf(
                "command" to "swipeWithDirection",
                "direction" to swipeDirection.name,
                "durationMs" to durationMs.toString()
            )
        ) {
            val deviceInfo = deviceInfo()
            when (swipeDirection) {
                SwipeDirection.UP -> {
                    val startX = (deviceInfo.widthGrid * 0.5f).toInt()
                    val startY = (deviceInfo.heightGrid * 0.5f).toInt()
                    val endX = (deviceInfo.widthGrid * 0.5f).toInt()
                    val endY = (deviceInfo.heightGrid * 0.1f).toInt()
                    directionalSwipe(
                        durationMs,
                        Point(startX, startY),
                        Point(endX, endY)
                    )
                }

                SwipeDirection.DOWN -> {
                    val startX = (deviceInfo.widthGrid * 0.5f).toInt()
                    val startY = (deviceInfo.heightGrid * 0.2f).toInt()
                    val endX = (deviceInfo.widthGrid * 0.5f).toInt()
                    val endY = (deviceInfo.heightGrid * 0.9f).toInt()
                    directionalSwipe(
                        durationMs,
                        Point(startX, startY),
                        Point(endX, endY)
                    )
                }

                SwipeDirection.RIGHT -> {
                    val startX = (deviceInfo.widthGrid * 0.1f).toInt()
                    val startY = (deviceInfo.heightGrid * 0.5f).toInt()
                    val endX = (deviceInfo.widthGrid * 0.9f).toInt()
                    val endY = (deviceInfo.heightGrid * 0.5f).toInt()
                    directionalSwipe(
                        durationMs,
                        Point(startX, startY),
                        Point(endX, endY)
                    )
                }

                SwipeDirection.LEFT -> {
                    val startX = (deviceInfo.widthGrid * 0.9f).toInt()
                    val startY = (deviceInfo.heightGrid * 0.5f).toInt()
                    val endX = (deviceInfo.widthGrid * 0.1f).toInt()
                    val endY = (deviceInfo.heightGrid * 0.5f).toInt()
                    directionalSwipe(
                        durationMs,
                        Point(startX, startY),
                        Point(endX, endY)
                    )
                }
            }
        }
    }

    override fun swipe(elementPoint: Point, direction: SwipeDirection, durationMs: Long) {
        metrics.measured(
            "operation",
            mapOf(
                "command" to "swipeWithElementPoint",
                "direction" to direction.name,
                "durationMs" to durationMs.toString()
            )
        ) {
            val deviceInfo = deviceInfo()
            when (direction) {
                SwipeDirection.UP -> {
                    val endY = (deviceInfo.heightGrid * 0.1f).toInt()
                    directionalSwipe(durationMs, elementPoint, Point(elementPoint.x, endY))
                }

                SwipeDirection.DOWN -> {
                    val endY = (deviceInfo.heightGrid * 0.9f).toInt()
                    directionalSwipe(durationMs, elementPoint, Point(elementPoint.x, endY))
                }

                SwipeDirection.RIGHT -> {
                    val endX = (deviceInfo.widthGrid * 0.9f).toInt()
                    directionalSwipe(durationMs, elementPoint, Point(endX, elementPoint.y))
                }

                SwipeDirection.LEFT -> {
                    val endX = (deviceInfo.widthGrid * 0.1f).toInt()
                    directionalSwipe(durationMs, elementPoint, Point(endX, elementPoint.y))
                }
            }
        }
    }

    private fun directionalSwipe(durationMs: Long, start: Point, end: Point) {
        metrics.measured(
            "operation",
            mapOf("command" to "directionalSwipe", "durationMs" to durationMs.toString())
        ) {
            swipe(start, end, durationMs)
        }
    }

    override fun backPress() {
        metrics.measured("operation", mapOf("command" to "backPress")) {
            appiumDriver.navigate().back()
            Thread.sleep(300)
        }
    }

    override fun hideKeyboard() {
        metrics.measured("operation", mapOf("command" to "hideKeyboard")) {
            appiumDriver.navigate().back()
            Thread.sleep(300)
            waitForAppToSettle(null, null)
        }
    }

    override fun takeScreenshot(out: Sink, compressed: Boolean) {
        metrics.measured(
            "operation",
            mapOf("command" to "takeScreenshot", "compressed" to compressed.toString())
        ) {
            runDeviceCall {
                val response = appiumDriver.getScreenshotAs(OutputType.BYTES)
                out.buffer().use {
                    it.write(response)
                }
            }
        }
    }

    @Suppress("SdCardPath")
    override fun startScreenRecording(out: Sink): ScreenRecording {
        // TODO: to be implemented with appium, NOT REQUIRED AS LAMBDATEST AUTOMATICALLY RECORDS THE SCREEN
        return metrics.measured("operation", mapOf("command" to "startScreenRecording")) {

            val deviceScreenRecordingPath = "/sdcard/maestro-screenrecording.mp4"

            val future = CompletableFuture.runAsync({
                val timeLimit = if (getDeviceApiLevel() >= 34) "--time-limit 0" else ""
                try {
                    shell("screenrecord $timeLimit --bit-rate '100000' $deviceScreenRecordingPath")
                } catch (e: IOException) {
                    throw IOException(
                        "Failed to capture screen recording on the device. Note that some Android emulators do not support screen recording. " +
                                "Try using a different Android emulator (eg. Pixel 5 / API 30)",
                        e,
                    )
                }
            }, Executors.newSingleThreadExecutor())

            object : ScreenRecording {
                override fun close() {
//                    dadb.shell("killall -INT screenrecord") // Ignore exit code
//                    future.get()
//                    Thread.sleep(3000)
//                    dadb.pull(out, deviceScreenRecordingPath)
                }
            }
        }
    }

    override fun inputText(text: String) {
        metrics.measured("operation", mapOf("command" to "inputText")) {
            val currentElement: WebElement = appiumDriver.switchTo().activeElement()
            currentElement.sendKeys(text)
        }
    }

    override fun openLink(link: String, appId: String?, autoVerify: Boolean, browser: Boolean) {
        metrics.measured(
            "operation",
            mapOf(
                "command" to "openLink",
                "appId" to appId,
                "autoVerify" to autoVerify.toString(),
                "browser" to browser.toString()
            )
        ) {
            if (browser) {
                openBrowser(link)
            } else {
                try {
                    val params = mapOf(
                        "url" to link,
                        "package" to appId
                    )
                    appiumDriver.executeScript("mobile:deepLink", params)

                    println("Successfully launched app with deep link: $link")

                } catch (e: Exception) {
                    println("Failed to launch app with deep link: ${e.message}")
                    e.printStackTrace()
                }
            }
        }
    }

    private fun openBrowser(link: String) {
        when {
            isPackageInstalled("com.android.chrome") -> {
                shell("am start -a android.intent.action.VIEW -d \"$link\" com.android.chrome")
            }

            isPackageInstalled("org.mozilla.firefox") -> {
                shell("am start -a android.intent.action.VIEW -d \"$link\" org.mozilla.firefox")
            }

            else -> {
                openLink(link, appId = "com.sitetracker", autoVerify = false, browser = false)
            }
        }
    }

    private fun installedPackages(): List<String> {
        try {
            // THIS IS NOT ENABLED ON LAMBDATEST
            val allPackages = appiumDriver.executeScript("mobile:listPackages") as List<String>
            println("--- All Installed Packages ---")
            allPackages.forEach { packageName -> println(packageName) }
            println("Total packages: ${allPackages.size}\n")
            return allPackages
        } catch (e: Exception) {
            println("Failed to launch app with deep link: ${e.message}")
            e.printStackTrace()
            return emptyList()
        } finally {
        }
    }

    override fun setLocation(latitude: Double, longitude: Double) {
        metrics.measured("operation", mapOf("command" to "setLocation")) {
//            shell("appops set dev.mobile.maestro android:mock_location allow")

            runDeviceCall {
                val remoteSessionAddress =
                    appiumDriver.remoteAddress.toString() + "/session/" + appiumDriver.sessionId
                val automationName =
                    appiumDriver.capabilities.getCapability("appium:automationName")
                io.appium.java_client.android.AndroidDriver(
                    URL(remoteSessionAddress),
                    automationName.toString()
                ).setLocation(
                    AndroidGeoLocation(
                        latitude,
                        longitude
                    )
                )
            }
        }
    }

    override fun eraseText(charactersToErase: Int) {
        metrics.measured(
            "operation",
            mapOf("command" to "eraseText", "charactersToErase" to charactersToErase.toString())
        ) {
            runDeviceCall {
                val currentElement: WebElement = appiumDriver.switchTo().activeElement()
                if (charactersToErase > 0) {
                    shell("input keyevent 67").repeat(charactersToErase)
                } else {
                    currentElement.clear()
                }
            }
        }
    }

    override fun setProxy(host: String, port: Int) {
        metrics.measured("operation", mapOf("command" to "setProxy")) {
            shell("""settings put global http_proxy "${host}:${port}"""")
            proxySet = true
        }
    }

    override fun resetProxy() {
        metrics.measured("operation", mapOf("command" to "resetProxy")) {
            shell("settings put global http_proxy :0")
        }
    }

    override fun isShutdown(): Boolean {
        return metrics.measured("operation", mapOf("command" to "isShutdown")) {
            appiumDriver.sessionId != null
        }
    }

    override fun isUnicodeInputSupported(): Boolean {
        return false
    }

    override fun waitForAppToSettle(
        initialHierarchy: ViewHierarchy?,
        appId: String?,
        timeoutMs: Int?
    ): ViewHierarchy? {
        return metrics.measured(
            "operation",
            mapOf(
                "command" to "waitForAppToSettle",
                "appId" to appId,
                "timeoutMs" to timeoutMs.toString()
            )
        ) {
            if (appId != null) {
                waitForWindowToSettle(appId, initialHierarchy, timeoutMs)
            } else {
                ScreenshotUtils.waitForAppToSettle(initialHierarchy, this, timeoutMs)
            }
        }
    }

    private fun waitForWindowToSettle(
        appId: String,
        initialHierarchy: ViewHierarchy?,
        timeoutMs: Int? = null
    ): ViewHierarchy {
        // TODO: to be implemented with appium
        val endTime = System.currentTimeMillis() + WINDOW_UPDATE_TIMEOUT_MS
        var hierarchy: ViewHierarchy? = null
        do {
            runDeviceCall {
                val windowUpdating = false
                if (windowUpdating) {
                    hierarchy =
                        ScreenshotUtils.waitForAppToSettle(initialHierarchy, this, timeoutMs)
                }
            }
        } while (System.currentTimeMillis() < endTime)

        return hierarchy ?: ScreenshotUtils.waitForAppToSettle(initialHierarchy, this)
    }

    override fun waitUntilScreenIsStatic(timeoutMs: Long): Boolean {
        return metrics.measured(
            "operation",
            mapOf("command" to "waitUntilScreenIsStatic", "timeoutMs" to timeoutMs.toString())
        ) {
            ScreenshotUtils.waitUntilScreenIsStatic(timeoutMs, SCREENSHOT_DIFF_THRESHOLD, this)
        }
    }

    override fun capabilities(): List<Capability> {
        return metrics.measured("operation", mapOf("command" to "capabilities")) {
            listOf(
                Capability.FAST_HIERARCHY
            )
        }
    }

    override fun setPermissions(appId: String, permissions: Map<String, String>) {
        metrics.measured("operation", mapOf("command" to "setPermissions", "appId" to appId)) {
            val mutable = permissions.toMutableMap()
            mutable.remove("all")?.let { value ->
                setAllPermissions(appId, value)
            }

            mutable.forEach { permission ->
                val permissionValue = translatePermissionValue(permission.value)
                translatePermissionName(permission.key).forEach { permissionName ->
                    setPermissionInternal(appId, permissionName, permissionValue)
                }
            }
        }
    }

    override fun addMedia(mediaFiles: List<File>) {
        metrics.measured(
            "operation",
            mapOf("command" to "addMedia", "mediaFilesCount" to mediaFiles.size.toString())
        ) {
            LOGGER.info("[Start] Adding media files")
            mediaFiles.forEach { addMediaToDevice(it) }
            LOGGER.info("[Done] Adding media files")
        }
    }

    override fun isAirplaneModeEnabled(): Boolean {
        return metrics.measured("operation", mapOf("command" to "isAirplaneModeEnabled")) {
            when (val result = shell("cmd connectivity airplane-mode").trim()) {
                "No shell command implementation.", "" -> {
                    LOGGER.debug("Falling back to old airplane mode read method")
                    when (val fallbackResult =
                        shell("settings get global airplane_mode_on").trim()) {
                        "0" -> false
                        "1" -> true
                        else -> throw IllegalStateException("Received invalid response from while trying to read airplane mode state: $fallbackResult")
                    }
                }

                "disabled" -> false
                "enabled" -> true
                else -> throw IllegalStateException("Received invalid response while trying to read airplane mode state: $result")
            }
        }
    }

    override fun setAirplaneMode(enabled: Boolean) {
        metrics.measured(
            "operation",
            mapOf("command" to "setAirplaneMode", "enabled" to enabled.toString())
        ) {
            // fallback to old way on API < 28
            if (getDeviceApiLevel() < 28) {
                val num = if (enabled) 1 else 0
                shell("settings put global airplane_mode_on $num")
                // We need to broadcast the change to really apply it
                broadcastAirplaneMode(enabled)
                return@measured
            }
            val value = if (enabled) "enable" else "disable"
            shell("cmd connectivity airplane-mode $value")
        }
    }

    private fun broadcastAirplaneMode(enabled: Boolean) {
        val command = "am broadcast -a android.intent.action.AIRPLANE_MODE --ez state $enabled"
        try {
            shell(command)
        } catch (e: IOException) {
            if (e.message?.contains("Security exception: Permission Denial:") == true) {
                try {
                    shell("su root $command")
                } catch (e: IOException) {
                    throw MaestroException.NoRootAccess("Failed to broadcast airplane mode change. Make sure to run an emulator with root access for API < 28")
                }
            }
        }
    }

    override fun setAndroidChromeDevToolsEnabled(enabled: Boolean) {
        this.chromeDevToolsEnabled = enabled
    }

    fun setDeviceLocale(country: String, language: String): Int {
        // TODO: to be implemented with appium
        return metrics.measured(
            "operation",
            mapOf("command" to "setDeviceLocale", "country" to country, "language" to language)
        ) {
//            dadb.shell("pm grant dev.mobile.maestro android.permission.CHANGE_CONFIGURATION")
//            val response =
//                dadb.shell("am broadcast -a dev.mobile.maestro.locale -n dev.mobile.maestro/.receivers.LocaleSettingReceiver --es lang $language --es country $country")
//            extractSetLocaleResult(response.output)
            10
        }
    }

    private fun extractSetLocaleResult(result: String): Int {
        val regex = Regex("result=(-?\\d+)")
        val match = regex.find(result)
        return match?.groups?.get(1)?.value?.toIntOrNull() ?: -1
    }

    private fun addMediaToDevice(mediaFile: File) {
        val namedSource = NamedSource(
            mediaFile.name,
            mediaFile.source(),
            mediaFile.extension,
            mediaFile.path
        )
        val remotePathAndroid = "/sdcard/${namedSource.name}"
        println("Attempting to push file (${namedSource.name}) to device.")
        if (appiumDriver is io.appium.java_client.android.AndroidDriver) {
            appiumDriver.pushFile(remotePathAndroid, File(namedSource.path))
        } else
            if (appiumDriver is io.appium.java_client.ios.IOSDriver) {
                // TODO: to be implemented for iOS
                appiumDriver.pushFile(remotePathAndroid, File(namedSource.path))
            }
        println("File '${namedSource.name}' pushed successfully to '$remotePathAndroid' on the device.")
    }

    private fun setAllPermissions(appId: String, permissionValue: String) {
        // TODO: to be implemented with appium
//        val permissionsResult = runCatching {
//            val apkFile = AndroidAppFiles.getApkFile(dadb, appId)
//            val permissions = ApkFile(apkFile).apkMeta.usesPermissions
//            apkFile.delete()
//            permissions
//        }
//        if (permissionsResult.isSuccess) {
//            permissionsResult.getOrNull()?.let {
//                it.forEach { permission ->
//                    setPermissionInternal(appId, permission, translatePermissionValue(permissionValue))
//                }
//            }
//        }
    }

    private fun setPermissionInternal(appId: String, permission: String, permissionValue: String) {
        // TODO: to be implemented with appium
        try {
//            dadb.shell("pm $permissionValue $appId $permission")
        } catch (exception: Exception) {
            /* no-op */
        }
    }

    private fun translatePermissionName(name: String): List<String> {
        return when (name) {
            "location" -> listOf(
                "android.permission.ACCESS_FINE_LOCATION",
                "android.permission.ACCESS_COARSE_LOCATION",
            )

            "camera" -> listOf("android.permission.CAMERA")
            "contacts" -> listOf(
                "android.permission.READ_CONTACTS",
                "android.permission.WRITE_CONTACTS"
            )

            "phone" -> listOf(
                "android.permission.CALL_PHONE",
                "android.permission.ANSWER_PHONE_CALLS",
            )

            "microphone" -> listOf(
                "android.permission.RECORD_AUDIO"
            )

            "bluetooth" -> listOf(
                "android.permission.BLUETOOTH_CONNECT",
                "android.permission.BLUETOOTH_SCAN",
            )

            "storage" -> listOf(
                "android.permission.WRITE_EXTERNAL_STORAGE",
                "android.permission.READ_EXTERNAL_STORAGE"
            )

            "notifications" -> listOf(
                "android.permission.POST_NOTIFICATIONS"
            )

            "medialibrary" -> listOf(
                "android.permission.WRITE_EXTERNAL_STORAGE",
                "android.permission.READ_EXTERNAL_STORAGE",
                "android.permission.READ_MEDIA_AUDIO",
                "android.permission.READ_MEDIA_IMAGES",
                "android.permission.READ_MEDIA_VIDEO"
            )

            "calendar" -> listOf(
                "android.permission.WRITE_CALENDAR",
                "android.permission.READ_CALENDAR"
            )

            "sms" -> listOf(
                "android.permission.READ_SMS",
                "android.permission.RECEIVE_SMS",
                "android.permission.SEND_SMS"
            )

            else -> listOf(name.replace("[^A-Za-z0-9._]+".toRegex(), ""))
        }
    }

    private fun translatePermissionValue(value: String): String {
        return when (value) {
            "allow" -> "grant"
            "deny" -> "revoke"
            "unset" -> "revoke"
            else -> "revoke"
        }
    }

    private fun mapHierarchy(node: Node): TreeNode {
        val attributes = if (node is Element) {
            val attributesBuilder = mutableMapOf<String, String>()

            if (node.hasAttribute("text")) {
                val text = node.getAttribute("text")
                attributesBuilder["text"] = text
            }

            if (node.hasAttribute("content-desc")) {
                attributesBuilder["accessibilityText"] = node.getAttribute("content-desc")
            }

            if (node.hasAttribute("hintText")) {
                attributesBuilder["hintText"] = node.getAttribute("hintText")
            }

            if (node.hasAttribute("class") && node.getAttribute("class") == TOAST_CLASS_NAME) {
                attributesBuilder["ignoreBoundsFiltering"] = true.toString()
            } else {
                attributesBuilder["ignoreBoundsFiltering"] = false.toString()
            }

            if (node.hasAttribute("resource-id")) {
                attributesBuilder["resource-id"] = node.getAttribute("resource-id")
            }

            if (node.hasAttribute("clickable")) {
                attributesBuilder["clickable"] = node.getAttribute("clickable")
            }

            if (node.hasAttribute("bounds")) {
                attributesBuilder["bounds"] = node.getAttribute("bounds")
            }

            if (node.hasAttribute("enabled")) {
                attributesBuilder["enabled"] = node.getAttribute("enabled")
            }

            if (node.hasAttribute("focused")) {
                attributesBuilder["focused"] = node.getAttribute("focused")
            }

            if (node.hasAttribute("checked")) {
                attributesBuilder["checked"] = node.getAttribute("checked")
            }

            if (node.hasAttribute("scrollable")) {
                attributesBuilder["scrollable"] = node.getAttribute("scrollable")
            }

            if (node.hasAttribute("selected")) {
                attributesBuilder["selected"] = node.getAttribute("selected")
            }

            if (node.hasAttribute("class")) {
                attributesBuilder["class"] = node.getAttribute("class")
            }

            attributesBuilder
        } else {
            emptyMap()
        }

        val children = mutableListOf<TreeNode>()
        val childNodes = node.childNodes
        (0 until childNodes.length).forEach { i ->
            children += mapHierarchy(childNodes.item(i))
        }

        return TreeNode(
            attributes = attributes.toMutableMap(),
            children = children,
            clickable = node.getBoolean("clickable"),
            enabled = node.getBoolean("enabled"),
            focused = node.getBoolean("focused"),
            checked = node.getBoolean("checked"),
            selected = node.getBoolean("selected"),
        )
    }

    private fun Node.getBoolean(name: String): Boolean? {
        return (this as? Element)
            ?.getAttribute(name)
            ?.let { it == "true" }
    }

    fun installMaestroDriverApp() {
        metrics.measured("operation", mapOf("command" to "installMaestroDriverApp")) {
            uninstallMaestroDriverApp()

            val maestroAppApk = File.createTempFile("maestro-app", ".apk")

            Maestro::class.java.getResourceAsStream("/maestro-app.apk")?.let {
                val bufferedSink = maestroAppApk.sink().buffer()
                bufferedSink.writeAll(it.source())
                bufferedSink.flush()
            }

            install(maestroAppApk)
            if (!isPackageInstalled("dev.mobile.maestro")) {
                throw IllegalStateException("dev.mobile.maestro was not installed")
            }
            maestroAppApk.delete()
        }
    }

    private fun installMaestroServerApp() {
        uninstallMaestroServerApp()

        val maestroServerApk = File.createTempFile("maestro-server", ".apk")

        Maestro::class.java.getResourceAsStream("/maestro-server.apk")?.let {
            val bufferedSink = maestroServerApk.sink().buffer()
            bufferedSink.writeAll(it.source())
            bufferedSink.flush()
        }

        install(maestroServerApk)
        if (!isPackageInstalled("dev.mobile.maestro.test")) {
            throw IllegalStateException("dev.mobile.maestro.test was not installed")
        }
        maestroServerApk.delete()
    }

    private fun installMaestroApks() {
        installMaestroDriverApp()
        installMaestroServerApp()
    }

    fun uninstallMaestroDriverApp() {
        metrics.measured("operation", mapOf("command" to "uninstallMaestroDriverApp")) {
            try {
                if (isPackageInstalled("dev.mobile.maestro")) {
                    uninstall("dev.mobile.maestro")
                }
            } catch (e: IOException) {
                logger.warn("Failed to check or uninstall maestro driver app: ${e.message}")
                // Continue with cleanup even if we can't check package status
                try {
                    uninstall("dev.mobile.maestro")
                } catch (e2: IOException) {
                    logger.warn("Failed to uninstall maestro driver app: ${e2.message}")
                    // Just log and continue, don't throw
                }
            }
        }
    }

    private fun uninstallMaestroServerApp() {
        try {
            if (isPackageInstalled("dev.mobile.maestro.test")) {
                uninstall("dev.mobile.maestro.test")
            }
        } catch (e: IOException) {
            logger.warn("Failed to check or uninstall maestro server app: ${e.message}")
            // Continue with cleanup even if we can't check package status
            try {
                uninstall("dev.mobile.maestro.test")
            } catch (e2: IOException) {
                logger.warn("Failed to uninstall maestro server app: ${e2.message}")
                // Just log and continue, don't throw
            }
        }
    }

    private fun uninstallMaestroApks() {
        uninstallMaestroDriverApp()
        uninstallMaestroServerApp()
    }

    private fun install(apkFile: File) {
        // TODO: to be implemented with appium
        try {
//            dadb.install(apkFile)
        } catch (installError: IOException) {
            throw IOException(
                "Failed to install apk " + apkFile + ": " + installError.message,
                installError
            )
        }
    }

    // TODO: to be implemented with appium
    private fun uninstall(packageName: String) {
        try {
//            dadb.uninstall(packageName)
        } catch (error: IOException) {
            throw IOException(
                "Failed to uninstall package " + packageName + ": " + error.message,
                error
            )
        }
    }

    private fun isPackageInstalled(packageName: String): Boolean {
        try {
            if (appiumDriver is io.appium.java_client.android.AndroidDriver) {
                return appiumDriver.isAppInstalled(packageName)
            } else if (appiumDriver is io.appium.java_client.ios.IOSDriver) {
                return appiumDriver.isAppInstalled(packageName)
            }
            throw IOException("Unsupported driver type")
        } catch (e: IOException) {
            logger.warn("Failed to check if package $packageName is installed: ${e.message}")
            // If we can't check, we'll assume it's not installed
            throw e
        }
    }

    private fun shell(command: String): String {
        val splitCommand = command.split(" ")
        val commandMap = mapOf(
            "command" to splitCommand[0],
            "args" to splitCommand.subList(1, splitCommand.size)
        )
        val pmListPackagesResult =
            appiumDriver.executeScript("mobile:shell", commandMap) as Map<*, *>
        return pmListPackagesResult["stdout"].toString()
    }

    private fun getStartupTimeout(): Long = runCatching {
        System.getenv(MAESTRO_DRIVER_STARTUP_TIMEOUT).toLong()
    }.getOrDefault(SERVER_LAUNCH_TIMEOUT_MS)

    private fun AdbShellStream?.successfullyStarted(): Boolean {
        val output = this?.read()
        return when {
            output is AdbShellPacket.StdError -> false
            output.toString().contains("FAILED", true) -> false
            output.toString().contains("UNABLE", true) -> false
            else -> true
        }
    }

    private fun <T> runDeviceCall(call: () -> T): T {
        return try {
            call()
        } catch (throwable: StatusRuntimeException) {
            val status = Status.fromThrowable(throwable)
            when (status.code) {
                Status.Code.DEADLINE_EXCEEDED -> {
                    LOGGER.error("Device call failed on appium with $status", throwable)
                    closed = true
                    throw MaestroException.DriverTimeout("Appium driver unreachable")
                }

                Status.Code.UNAVAILABLE -> {
                    if (throwable.cause is IOException || throwable.message?.contains(
                            "io exception",
                            ignoreCase = true
                        ) == true
                    ) {
                        LOGGER.error("Not able to reach the gRPC server while doing Appium device call")
                        closed = true
                        throw throwable
                    } else {
                        LOGGER.error(
                            "Received UNAVAILABLE status with message: ${throwable.message} while doing appium device call",
                            throwable
                        )
                        throw throwable
                    }
                }

                else -> {
                    LOGGER.error(
                        "Unexpected error: ${status.code} - ${throwable.message} and cause ${throwable.cause} while doing appium device call",
                        throwable
                    )
                    throw throwable
                }
            }
        }
    }


    companion object {

        private const val SERVER_LAUNCH_TIMEOUT_MS = 15000L
        private const val MAESTRO_DRIVER_STARTUP_TIMEOUT = "MAESTRO_DRIVER_STARTUP_TIMEOUT"
        private const val WINDOW_UPDATE_TIMEOUT_MS = 750

        private val REGEX_OPTIONS =
            setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL, RegexOption.MULTILINE)

        private val LOGGER = LoggerFactory.getLogger(AppiumDriver::class.java)

        private const val TOAST_CLASS_NAME = "android.widget.Toast"
        private val PORT_TO_FORWARDER = mutableMapOf<Int, AutoCloseable>()
        private val PORT_TO_ALLOCATION_POINT = mutableMapOf<Int, String>()
        private const val SCREENSHOT_DIFF_THRESHOLD = 0.005
        private const val CHUNK_SIZE = 1024L * 1024L * 3L
    }
}