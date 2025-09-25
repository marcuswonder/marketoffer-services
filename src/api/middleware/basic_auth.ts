import { Request, Response, NextFunction } from "express";

export function basicAuth(req: Request, res: Response, next: NextFunction) {
  const user = process.env.ADMIN_USER || "";
  const pass = process.env.ADMIN_PASS || "";
  if (!user || !pass) {
    return res.status(500).send("Basic auth credentials not configured");
  }
  const header = req.headers.authorization || "";
  const [scheme, encoded] = header.split(" ");
  if (!scheme || scheme.toLowerCase() !== "basic" || !encoded) {
    res.setHeader("WWW-Authenticate", "Basic realm=\"Admin\"");
    return res.status(401).send("Authentication required");
  }
  const decoded = Buffer.from(encoded, "base64").toString();
  const [incomingUser, incomingPass] = decoded.split(":");
  if (incomingUser === user && incomingPass === pass) {
    return next();
  }
  res.setHeader("WWW-Authenticate", "Basic realm=\"Admin\"");
  return res.status(401).send("Invalid credentials");
}
