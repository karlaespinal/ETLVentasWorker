namespace ETLVentasWorker.Models;

public class StagingRecord
{
    public int Id { get; set; }
    public string? Nombre { get; set; }
    public string? Comentario { get; set; }
    public string? Descripcion { get; set; }
    public string? Tipo { get; set; }
}