/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package ru.myx.bdbje.upgrade;

import java.io.File;

import ru.myx.ae3.Engine;

/** In JE 5.0 the internal log format changed. Before upgrading environments to JE 5.0, this utility
 * must first be run using JE 4.1. */
public class DbPreUpgrade_4_1 {

	/** Internal
	 *
	 * @param envFolder
	 * @throws Exception */
	public static void upgrade(final File envFolder) throws Exception {

		final File jarFile = new File(Engine.PATH_PUBLIC, "resources/data/bdbj/upgrade/je-4.1.21.jar");
		final File javaFile = new File(new File(System.getProperty("java.home")), "bin/java");
		final String javaPath = javaFile.getAbsolutePath();
		final String jarPath = jarFile.getAbsolutePath();
		final String envPath = envFolder.getAbsolutePath();
		final ProcessBuilder builder = new ProcessBuilder();
		builder.command(javaPath, "-jar", jarPath, "DbPreUpgrade_4_1", "-h", envPath);
		builder.inheritIO();
		builder.directory(Engine.PATH_PRIVATE);
		System.out.println("BDBJE-UPGRADE: " + String.join(" ", builder.command()));
		builder.start().waitFor();
	}

	/** prevent instantiation */
	private DbPreUpgrade_4_1() {

		// ignore
	}
}
