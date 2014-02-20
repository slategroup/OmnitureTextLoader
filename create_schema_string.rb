file = File.open("schema#{ARGV[0]}.txt", "r")

lines = []
while (line = file.gets)
  lines << line.chomp if !line.start_with? "--"
end

puts lines.join("")