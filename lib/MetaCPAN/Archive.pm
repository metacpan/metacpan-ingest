package MetaCPAN::Archive;

use strict;
use warnings;

use Archive::Any ();
use Path::Tiny;

sub new {
    my ( $class, %args ) = @_;
    my $file = $args{file}
        or die "Missing file\n";

    my $archive = Archive::Any->new($file);

    log_error { $file, ' is being impolite' } if $archive->is_impolite;

    log_error { $file, ' is being naughty' } if $archive->is_naughty;

    return bless {
        file    => $file,
        archive => $archive,
    }, $class;
}

sub is_impolite {
    my ($self) = @_;
    $self->{archive}->is_impolite;
}

sub files {
    my ($self) = @_;
    return $self->{archive}->files;
}

sub extract {
    my ($self) = @_;
    my $extract_dir = $self->_extract_dir;
    $self->{archive}->extract($extract_dir);
    $self->{extract_dir} = $extract_dir;
    return $extract_dir;
}

sub _extract_dir {
    my ($self) = @_;
    my $scratch_disk = '/mnt/scratch_disk';
    return -d $scratch_disk
        ? Path::Tiny->tempdir('/mnt/scratch_disk/tempXXXXX')
        : Path::Tiny->tempdir;
}

1;
