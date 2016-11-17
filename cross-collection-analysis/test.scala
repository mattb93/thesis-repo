val testArray = Array("@twitter", "twitter", "@lol", "derp", "hello@")

testArray.filter(x => ! """@.*""".r.pattern.matcher(x).matches)
